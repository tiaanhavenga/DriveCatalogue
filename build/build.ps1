python -m pip install -U uv
uv pip install --system -r requirements.txt
pyinstaller build/drive_catalogue.spec
Write-Host "Built to dist/"
