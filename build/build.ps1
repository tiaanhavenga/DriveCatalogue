python -m pip install -U pip wheel
pip install -r requirements.txt
python -m PyInstaller build/drive_catalogue.spec
Write-Host "Built to dist/"
