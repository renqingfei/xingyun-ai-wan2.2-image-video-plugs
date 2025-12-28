import argparse
import json
import mimetypes
import os
import secrets
import sys
import time
import uuid
from copy import deepcopy
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.parse import urljoin, urlparse
from urllib.request import Request, urlopen


def _read_json(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def _log(message: str, request_id: str | None = None):
    ts = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    rid = f" rid={request_id}" if request_id else ""
    print(f"[{ts}]{rid} {message}", flush=True)


def _write_json_bytes(obj: object) -> bytes:
    return json.dumps(obj, ensure_ascii=False).encode("utf-8")


def _post_json(url: str, payload: dict, timeout_s: int = 120) -> dict:
    data = _write_json_bytes(payload)
    req = Request(
        url=url,
        data=data,
        headers={"Content-Type": "application/json; charset=utf-8"},
        method="POST",
    )
    with urlopen(req, timeout=timeout_s) as resp:
        body = resp.read()
        if not body:
            return {}
        return json.loads(body.decode("utf-8"))


def _get_json(url: str, timeout_s: int = 120) -> dict:
    req = Request(url=url, method="GET")
    with urlopen(req, timeout=timeout_s) as resp:
        body = resp.read()
        if not body:
            return {}
        return json.loads(body.decode("utf-8"))


def _multipart_form_data(fields: list[tuple[str, str]], files: list[tuple[str, str, bytes]]):
    boundary = "----PythonMultipartBoundary" + uuid.uuid4().hex
    crlf = "\r\n"
    chunks: list[bytes] = []

    for name, value in fields:
        chunks.append(f"--{boundary}{crlf}".encode("utf-8"))
        chunks.append(f'Content-Disposition: form-data; name="{name}"{crlf}{crlf}'.encode("utf-8"))
        chunks.append(value.encode("utf-8"))
        chunks.append(crlf.encode("utf-8"))

    for fieldname, filename, content in files:
        content_type = mimetypes.guess_type(filename)[0] or "application/octet-stream"
        chunks.append(f"--{boundary}{crlf}".encode("utf-8"))
        chunks.append(
            f'Content-Disposition: form-data; name="{fieldname}"; filename="{filename}"{crlf}'.encode("utf-8")
        )
        chunks.append(f"Content-Type: {content_type}{crlf}{crlf}".encode("utf-8"))
        chunks.append(content)
        chunks.append(crlf.encode("utf-8"))

    chunks.append(f"--{boundary}--{crlf}".encode("utf-8"))
    body = b"".join(chunks)
    content_type_header = f"multipart/form-data; boundary={boundary}"
    return content_type_header, body


def upload_image_to_comfyui(comfyui_url: str, image_path: str, timeout_s: int = 120) -> dict:
    with open(image_path, "rb") as f:
        content = f.read()

    filename = os.path.basename(image_path)
    content_type, body = _multipart_form_data(fields=[("type", "input")], files=[("image", filename, content)])
    url = urljoin(comfyui_url.rstrip("/") + "/", "upload/image")
    req = Request(url=url, data=body, headers={"Content-Type": content_type}, method="POST")
    with urlopen(req, timeout=timeout_s) as resp:
        resp_body = resp.read()
        if not resp_body:
            return {}
        return json.loads(resp_body.decode("utf-8"))


def _coerce_positive_int(value) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return None
    try:
        v = int(value)
    except (TypeError, ValueError):
        return None
    return v if v > 0 else None


def _workflow_path() -> str:
    base_dir = getattr(sys, "_MEIPASS", os.path.dirname(os.path.abspath(__file__)))
    return os.path.join(base_dir, "万象2.2图生视频.json")


def build_comfyui_workflow(
    request_json: dict,
    *,
    uploaded_image_name: str | None,
    template: dict,
) -> dict:
    wf = deepcopy(template)

    prompt_text = (request_json.get("prompt") or "").strip()
    if "93" in wf and "inputs" in wf["93"]:
        wf["93"]["inputs"]["text"] = prompt_text

    if uploaded_image_name:
        if "97" in wf and "inputs" in wf["97"]:
            wf["97"]["inputs"]["image"] = uploaded_image_name

    req_width = _coerce_positive_int(request_json.get("width"))
    req_height = _coerce_positive_int(request_json.get("height"))
    if not req_width or not req_height:
        raise ValueError("width and height are required")
    width, height = req_width, req_height
    if "98" in wf and "inputs" in wf["98"]:
        wf["98"]["inputs"]["width"] = int(width)
        wf["98"]["inputs"]["height"] = int(height)

    if "86" in wf and "inputs" in wf["86"]:
        wf["86"]["inputs"]["noise_seed"] = secrets.randbits(63)

    return wf


def _safe_comfyui_url(raw: str) -> str:
    parsed = urlparse(raw)
    if parsed.scheme not in ("http", "https"):
        raise ValueError("comfyui_url must start with http:// or https://")
    if not parsed.netloc:
        raise ValueError("comfyui_url is missing host")
    return raw.rstrip("/") + "/"


def _wait_for_video_history(
    comfyui_url: str,
    prompt_id: str,
    save_video_node_ids: list[str],
    *,
    timeout_s: int = 600,
    interval_s: float = 1.0,
    request_id: str | None = None,
) -> dict:
    deadline = time.time() + timeout_s
    history_url = urljoin(comfyui_url, f"history/{prompt_id}")
    last_seen = None
    poll_count = 0

    while time.time() < deadline:
        poll_count += 1
        _log(f"轮询历史接口({poll_count}): GET {history_url}", request_id)
        per_request_timeout = min(120, max(1, int(deadline - time.time())))
        data = _get_json(history_url, timeout_s=per_request_timeout)
        if isinstance(data, dict) and prompt_id in data and isinstance(data[prompt_id], dict):
            item = data[prompt_id]
            outputs = item.get("outputs") or {}
            if isinstance(outputs, dict):
                for node_id in save_video_node_ids:
                    out = outputs.get(node_id)
                    if isinstance(out, dict):
                        for key in ("images", "videos", "gifs"):
                            files = out.get(key)
                            if isinstance(files, list) and files:
                                return item
                if outputs and outputs != last_seen:
                    last_seen = outputs
        time.sleep(interval_s)

    raise TimeoutError("timeout waiting for video output from ComfyUI")


def _extract_first_video_file(history_item: dict, save_video_node_ids: list[str]) -> dict | None:
    outputs = history_item.get("outputs") or {}
    if not isinstance(outputs, dict):
        return None

    def pick_from(out: dict) -> dict | None:
        for key in ("images", "videos", "gifs"):
            files = out.get(key)
            if isinstance(files, list) and files:
                f = files[0]
                if isinstance(f, dict) and (f.get("filename") or f.get("name")):
                    return f
        return None

    for node_id in save_video_node_ids:
        out = outputs.get(node_id)
        if isinstance(out, dict):
            picked = pick_from(out)
            if picked:
                return picked

    for out in outputs.values():
        if isinstance(out, dict):
            picked = pick_from(out)
            if picked:
                return picked
    return None


def _build_view_url(comfyui_url: str, file_info: dict) -> str:
    filename = file_info.get("filename") or file_info.get("name") or ""
    subfolder = file_info.get("subfolder") or ""
    file_type = file_info.get("type") or "output"
    if not filename:
        raise ValueError("missing filename in ComfyUI history output")
    query = {"filename": filename, "type": file_type}
    if subfolder:
        query["subfolder"] = subfolder
    return urljoin(comfyui_url, "view") + "?" + urlencode(query)


class ProxyHandler(BaseHTTPRequestHandler):
    server_version = "XingyunComfyProxy/1.0"

    def _send_json(self, status: int, payload: dict):
        data = _write_json_bytes(payload)
        try:
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)
        except (BrokenPipeError, ConnectionResetError, ConnectionAbortedError, OSError):
            return

    def log_message(self, format, *args):
        return

    def do_GET(self):
        self._send_json(404, {"error": {"message": "not found", "type": "not_found"}})

    def do_POST(self):
        request_id = uuid.uuid4().hex[:12]
        try:
            _log(f"请求进入: {self.command} {self.path}", request_id)
            overall_deadline = time.time() + float(getattr(self.server, "request_timeout_s", 600))

            def remaining_s() -> int:
                return max(0, int(overall_deadline - time.time()))

            if self.path.rstrip("/") != "/v1/videos/generations":
                self._send_json(404, {"error": {"message": "not found", "type": "not_found"}})
                _log("路径不匹配，返回 404", request_id)
                return

            try:
                content_length = int(self.headers.get("Content-Length", "0"))
            except ValueError:
                content_length = 0
            body = self.rfile.read(content_length) if content_length > 0 else b""

            try:
                req_json = json.loads(body.decode("utf-8") if body else "{}")
            except json.JSONDecodeError:
                self._send_json(400, {"error": {"message": "invalid json", "type": "invalid_request"}})
                _log("请求体 JSON 解析失败，返回 400", request_id)
                return

            try:
                comfyui_url = _safe_comfyui_url(str(req_json.get("comfyui_url") or ""))
            except ValueError as e:
                self._send_json(400, {"error": {"message": str(e), "type": "invalid_request"}})
                _log(f"comfyui_url 校验失败: {e}，返回 400", request_id)
                return

            images = req_json.get("images") or []
            first_image = images[0] if isinstance(images, list) and images else None

            req_width = _coerce_positive_int(req_json.get("width"))
            req_height = _coerce_positive_int(req_json.get("height"))
            if not req_width or not req_height:
                self._send_json(
                    400,
                    {"error": {"message": "width and height are required", "type": "invalid_request"}},
                )
                _log("缺少 width/height，返回 400", request_id)
                return

            uploaded_image_name = None
            if not isinstance(first_image, str) or not first_image.strip():
                self._send_json(
                    400,
                    {"error": {"message": "images[0] must be a local file path", "type": "invalid_request"}},
                )
                _log("缺少 images[0] 本地路径，返回 400", request_id)
                return

            local_image_path = first_image
            if not os.path.exists(local_image_path):
                self._send_json(
                    400,
                    {"error": {"message": f"local image not found: {local_image_path}", "type": "invalid_request"}},
                )
                _log(f"本地图片不存在: {local_image_path}，返回 400", request_id)
                return

            try:
                _log(f"步骤1: 上传图片 -> {urljoin(comfyui_url, 'upload/image')}", request_id)
                if remaining_s() <= 0:
                    raise TimeoutError("request timeout")
                upload_resp = upload_image_to_comfyui(comfyui_url, local_image_path, timeout_s=max(1, remaining_s()))
                name = upload_resp.get("name") or upload_resp.get("filename")
                subfolder = upload_resp.get("subfolder") or ""
                if subfolder:
                    uploaded_image_name = f"{subfolder}/{name}"
                else:
                    uploaded_image_name = name
                _log(f"图片上传完成: {uploaded_image_name}", request_id)
            except (HTTPError, URLError, TimeoutError) as e:
                status = 504 if isinstance(e, TimeoutError) else 502
                err_type = "upstream_timeout" if isinstance(e, TimeoutError) else "upstream_error"
                self._send_json(status, {"error": {"message": f"upload image failed: {e}", "type": err_type}})
                _log(f"图片上传失败，返回 {status}: {e}", request_id)
                return

            try:
                _log("步骤2: 构建工作流并替换参数", request_id)
                template = self.server.workflow_template
                workflow = build_comfyui_workflow(req_json, uploaded_image_name=uploaded_image_name, template=template)
                _log(f"工作流构建完成: width={req_width}, height={req_height}", request_id)
            except Exception as e:
                self._send_json(500, {"error": {"message": f"build workflow failed: {e}", "type": "server_error"}})
                _log(f"工作流构建失败，返回 500: {e}", request_id)
                return

            client_id = str(req_json.get("client_id") or "123asdfasfasd122421412")
            payload = {"client_id": client_id, "prompt": workflow}

            try:
                _log(f"步骤3: 提交队列 -> {urljoin(comfyui_url, 'api/prompt')}", request_id)
                if remaining_s() <= 0:
                    raise TimeoutError("request timeout")
                resp = _post_json(urljoin(comfyui_url, "api/prompt"), payload, timeout_s=max(1, remaining_s()))
            except (HTTPError, URLError, TimeoutError) as e:
                status = 504 if isinstance(e, TimeoutError) else 502
                err_type = "upstream_timeout" if isinstance(e, TimeoutError) else "upstream_error"
                self._send_json(status, {"error": {"message": f"upstream comfyui failed: {e}", "type": err_type}})
                _log(f"提交队列失败，返回 {status}: {e}", request_id)
                return

            prompt_id = resp.get("prompt_id") if isinstance(resp, dict) else None
            if not prompt_id:
                self._send_json(
                    502,
                    {"error": {"message": f"invalid ComfyUI response: {resp}", "type": "upstream_error"}},
                )
                _log(f"ComfyUI 返回缺少 prompt_id，返回 502: {resp}", request_id)
                return
            _log(f"队列提交成功: prompt_id={prompt_id}", request_id)

            try:
                _log(f"步骤4: 轮询结果 -> {urljoin(comfyui_url, f'history/{prompt_id}')}", request_id)
                if remaining_s() <= 0:
                    raise TimeoutError("request timeout")
                history_item = _wait_for_video_history(
                    comfyui_url,
                    str(prompt_id),
                    self.server.save_video_node_ids,
                    timeout_s=min(int(self.server.poll_timeout_s), max(1, remaining_s())),
                    interval_s=self.server.poll_interval_s,
                    request_id=request_id,
                )
                file_info = _extract_first_video_file(history_item, self.server.save_video_node_ids)
                if not file_info:
                    raise ValueError("no video output found in ComfyUI history")
                view_url = _build_view_url(comfyui_url, file_info)
                _log(f"步骤5: 生成下载/查看接口 -> GET {view_url}", request_id)
            except (HTTPError, URLError, TimeoutError, ValueError) as e:
                status = 504 if isinstance(e, TimeoutError) else 502
                err_type = "upstream_timeout" if isinstance(e, TimeoutError) else "upstream_error"
                self._send_json(
                    status,
                    {"error": {"message": f"fetch result failed: {e}", "type": err_type, "prompt_id": prompt_id}},
                )
                _log(f"拉取结果失败，返回 {status}: {e}", request_id)
                return

            public_url = view_url

            response_payload = {
                "created": int(time.time()),
                "data": [
                    {
                        "url": public_url,
                        "revised_prompt": str(req_json.get("prompt") or ""),
                    }
                ],
            }
            self._send_json(200, response_payload)
            _log("响应完成: 200", request_id)
        except (BrokenPipeError, ConnectionResetError, ConnectionAbortedError, OSError) as e:
            _log(f"客户端断开连接，忽略发送响应异常: {e}", request_id)
            return
        except Exception as e:
            self._send_json(500, {"error": {"message": f"server error: {e}", "type": "server_error"}})
            _log(f"未捕获异常，返回 500: {e}", request_id)
            return


class ProxyServer(HTTPServer):
    def __init__(
        self,
        server_address,
        RequestHandlerClass,
        workflow_template: dict,
        *,
        poll_timeout_s: int = 600,
        poll_interval_s: float = 1.0,
        request_timeout_s: int = 600,
    ):
        super().__init__(server_address, RequestHandlerClass)
        self.workflow_template = workflow_template
        self.save_video_node_ids = [
            node_id for node_id, node in workflow_template.items() if isinstance(node, dict) and node.get("class_type") == "SaveVideo"
        ]
        self.poll_timeout_s = poll_timeout_s
        self.poll_interval_s = poll_interval_s
        self.request_timeout_s = request_timeout_s


def _self_test():
    template = _read_json(_workflow_path())
    req = {
        "prompt": "test prompt",
        "images": [],
        "ratio": "9:16",
        "resolution": "720p",
        "model": "video-3.0",
        "sample_strength": 0.7,
        "comfyui_url": "http://localhost:8188/",
    }
    wf = build_comfyui_workflow(req, uploaded_image_name=None, template=template)
    assert wf["93"]["inputs"]["text"] == "test prompt"
    assert isinstance(wf["98"]["inputs"]["width"], int)
    assert isinstance(wf["98"]["inputs"]["height"], int)


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=13004)
    parser.add_argument("--poll-timeout", type=int, default=600)
    parser.add_argument("--poll-interval", type=float, default=5.0)
    parser.add_argument("--request-timeout", type=int, default=600)
    parser.add_argument("--self-test", action="store_true")
    args = parser.parse_args(argv)

    if args.self_test:
        _self_test()
        return 0

    workflow_template = _read_json(_workflow_path())
    httpd = ProxyServer(
        (args.host, args.port),
        ProxyHandler,
        workflow_template=workflow_template,
        poll_timeout_s=int(args.poll_timeout),
        poll_interval_s=float(args.poll_interval),
        request_timeout_s=int(args.request_timeout),
    )
    print(f"启动成功，监听 {args.host}:{args.port}，接口 /v1/videos/generations", flush=True)
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        return 0
    finally:
        httpd.server_close()


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
