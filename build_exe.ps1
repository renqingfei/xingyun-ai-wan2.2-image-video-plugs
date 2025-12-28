python -m pip install --upgrade pip
python -m pip install pyinstaller
pyinstaller --clean -F -n "wan2.2" --add-data "万象2.2图生视频.json;." proxy_server.py
