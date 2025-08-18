@echo off
IF NOT EXIST .venv (
    py -m venv .venv
)
CALL .venv\Scripts\activate.bat
pip install -r requirements.txt
py -m PyInstaller --noconfirm --onefile --windowed --name DriveCatalogue_V5 DriveCatalogue_V5.py
