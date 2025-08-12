# build/drive_catalogue.spec
from PyInstaller.utils.hooks import collect_submodules, collect_dynamic_libs
from PyInstaller.building.build_main import Analysis, PYZ, EXE
import os

app_name = "DriveCatalogue"
entry = "app.py"  # your PySide6 entrypoint

hiddenimports = collect_submodules('PySide6') + [
    'PySide6.QtSvgWidgets', 'PySide6.QtPrintSupport'
]
binaries = collect_dynamic_libs('PySide6')

datas = [('assets', 'assets')]
# Only add ffprobe if present
if os.path.exists('ffmpeg/ffprobe.exe'):
    datas.append(('ffmpeg/ffprobe.exe', 'ffmpeg'))

a = Analysis(
    [entry],
    pathex=['.'],
    binaries=binaries,
    datas=datas,
    hiddenimports=hiddenimports,
    noarchive=False,
)

pyz = PYZ(a.pure)
exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.zipfiles,
    a.datas,
    name=app_name,
    console=False,
    icon='assets/app.ico' if os.path.exists('assets/app.ico') else None,
)
