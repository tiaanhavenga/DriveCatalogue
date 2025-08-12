import sys
from PySide6.QtWidgets import QApplication, QLabel
def main():
    app = QApplication(sys.argv)
    label = QLabel("Drive Catalogue – PySide6 test build")
    label.resize(420, 200)
    label.show()
    sys.exit(app.exec())
if __name__ == "__main__":
    main()
