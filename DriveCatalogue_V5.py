import os
import sys
import sqlite3
import hashlib
import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path
from datetime import datetime, time, timedelta
import json
import csv
import shutil
import ctypes
from ctypes import wintypes
import fnmatch

from PySide6.QtCore import (Qt, QThread, Signal, Slot, QObject, QTimer, QSize,
                            QDateTime)
from PySide6.QtWidgets import (QApplication, QMainWindow, QWidget, QVBoxLayout,
                               QHBoxLayout, QLabel, QPushButton, QLineEdit,
                               QTabWidget, QFileDialog, QMessageBox,
                               QTableWidget, QTableWidgetItem, QHeaderView,
                               QListWidget, QListWidgetItem, QProgressBar,
                               QFormLayout, QCheckBox, QSpinBox, QTimeEdit)
from PySide6.QtGui import QAction, QIcon, QColor, QPalette

from matplotlib.backends.backend_qtagg import FigureCanvasQTAgg as FigureCanvas
from matplotlib.figure import Figure

APP_NAME = "DriveCatalogue"
APP_DIR = Path(os.environ.get("LOCALAPPDATA", Path.home() / "AppData/Local")) / APP_NAME
DATA_DIR = APP_DIR / "data"
LOG_DIR = APP_DIR / "logs"
DB_PATH = DATA_DIR / "drive_catalogue.db"
LOG_PATH = LOG_DIR / "app.log"
CONFIG_TABLE = "app_config"

os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(LOG_DIR, exist_ok=True)

handler = RotatingFileHandler(LOG_PATH, maxBytes=1_000_000, backupCount=5)
logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s",
                    handlers=[handler])
logger = logging.getLogger("DriveCatalogue")

class Database:
    def __init__(self, path: Path):
        self.path = path
        self.conn = sqlite3.connect(self.path)
        self.conn.row_factory = sqlite3.Row
        self._init_db()

    def _init_db(self):
        c = self.conn.cursor()
        c.execute("PRAGMA journal_mode=WAL")
        c.execute("PRAGMA synchronous=NORMAL")
        c.execute("PRAGMA foreign_keys=ON")
        c.execute("""
            CREATE TABLE IF NOT EXISTS roots(
                id INTEGER PRIMARY KEY,
                alias TEXT UNIQUE NOT NULL,
                root_path TEXT NOT NULL,
                volume_serial INTEGER,
                first_seen_ts INTEGER,
                last_scan_ts INTEGER,
                scanned_total_capacity_bytes INTEGER,
                scanned_free_bytes INTEGER
            )
        """)
        c.execute("""
            CREATE TABLE IF NOT EXISTS files(
                id INTEGER PRIMARY KEY,
                root_id INTEGER,
                file_name TEXT,
                relative_path TEXT,
                size INTEGER,
                modified_time INTEGER,
                FOREIGN KEY(root_id) REFERENCES roots(id) ON DELETE CASCADE
            )
        """)
        c.execute("CREATE INDEX IF NOT EXISTS idx_files_root ON files(root_id)")
        c.execute("CREATE INDEX IF NOT EXISTS idx_files_name ON files(file_name)")
        c.execute("CREATE INDEX IF NOT EXISTS idx_files_size ON files(size)")
        c.execute("CREATE INDEX IF NOT EXISTS idx_files_mtime ON files(modified_time)")
        c.execute(f"""
            CREATE TABLE IF NOT EXISTS {CONFIG_TABLE}(
                key TEXT PRIMARY KEY,
                value TEXT
            )
        """)
        self.conn.commit()

    def get_config(self, key, default=None):
        c = self.conn.cursor()
        c.execute(f"SELECT value FROM {CONFIG_TABLE} WHERE key=?", (key,))
        row = c.fetchone()
        if row:
            return json.loads(row[0])
        return default

    def set_config(self, key, value):
        c = self.conn.cursor()
        c.execute(f"REPLACE INTO {CONFIG_TABLE}(key,value) VALUES(?,?)",
                  (key, json.dumps(value)))
        self.conn.commit()

    def add_root(self, alias, path, volume_serial):
        ts = int(datetime.utcnow().timestamp())
        c = self.conn.cursor()
        c.execute("INSERT INTO roots(alias, root_path, volume_serial, first_seen_ts) VALUES(?,?,?,?)",
                  (alias, path, volume_serial, ts))
        self.conn.commit()
        return c.lastrowid

    def update_root_scan(self, root_id, capacity, free):
        ts = int(datetime.utcnow().timestamp())
        c = self.conn.cursor()
        c.execute("UPDATE roots SET last_scan_ts=?, scanned_total_capacity_bytes=?, scanned_free_bytes=? WHERE id=?",
                  (ts, capacity, free, root_id))
        self.conn.commit()

    def get_root_by_alias(self, alias):
        c = self.conn.cursor()
        c.execute("SELECT * FROM roots WHERE alias=?", (alias,))
        return c.fetchone()

    def list_roots(self):
        c = self.conn.cursor()
        c.execute("SELECT * FROM roots")
        return c.fetchall()

    def insert_files_batch(self, rows):
        c = self.conn.cursor()
        c.executemany("""
            INSERT INTO files(root_id, file_name, relative_path, size, modified_time)
            VALUES(?,?,?,?,?)
        """, rows)
        self.conn.commit()

    def search_files(self, name_sub, min_size, max_size, extension, limit, offset):
        query = "SELECT f.file_name, f.size, f.modified_time, (r.root_path || '\\' || f.relative_path) as full_path, r.alias FROM files f JOIN roots r ON f.root_id=r.id WHERE 1=1"
        params = []
        if name_sub:
            query += " AND f.file_name LIKE ?"
            params.append(f"%{name_sub}%")
        if min_size is not None:
            query += " AND f.size >= ?"
            params.append(min_size)
        if max_size is not None:
            query += " AND f.size <= ?"
            params.append(max_size)
        if extension:
            query += " AND f.file_name LIKE ?"
            params.append(f"%.{extension}")
        query += " ORDER BY f.modified_time DESC LIMIT ? OFFSET ?"
        params.extend([limit, offset])
        c = self.conn.cursor()
        c.execute(query, params)
        return c.fetchall()

    def count_search(self, name_sub, min_size, max_size, extension):
        query = "SELECT COUNT(*) FROM files WHERE 1=1"
        params = []
        if name_sub:
            query += " AND file_name LIKE ?"
            params.append(f"%{name_sub}%")
        if min_size is not None:
            query += " AND size >= ?"
            params.append(min_size)
        if max_size is not None:
            query += " AND size <= ?"
            params.append(max_size)
        if extension:
            query += " AND file_name LIKE ?"
            params.append(f"%.{extension}")
        c = self.conn.cursor()
        c.execute(query, params)
        return c.fetchone()[0]

    def backup_db(self):
        backup_name = datetime.utcnow().strftime("backup_%Y%m%d_%H%M%S.db")
        dest = DB_PATH.parent / backup_name
        self.conn.commit()
        self.conn.close()
        shutil.copy(self.path, dest)
        self.conn = sqlite3.connect(self.path)
        self.conn.row_factory = sqlite3.Row
        self._init_db()
        return dest

    def export_csv(self, path):
        c = self.conn.cursor()
        with open(path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(['file_name','size','modified_time','relative_path','root_id'])
            for row in c.execute("SELECT file_name,size,modified_time,relative_path,root_id FROM files"):
                writer.writerow(row)

    def export_json(self, path):
        c = self.conn.cursor()
        roots = []
        for r in c.execute("SELECT * FROM roots"):
            roots.append(dict(r))
        with open(path, 'w', encoding='utf-8') as f:
            json.dump(roots, f, indent=2)


def get_volume_serial(path):
    kernel32 = ctypes.windll.kernel32
    volume_name_buf = ctypes.create_unicode_buffer(1024)
    fs_name_buf = ctypes.create_unicode_buffer(1024)
    serial = wintypes.DWORD()
    max_comp_len = wintypes.DWORD()
    file_sys_flags = wintypes.DWORD()
    rc = kernel32.GetVolumeInformationW(ctypes.c_wchar_p(path), volume_name_buf,
                                        ctypes.sizeof(volume_name_buf),
                                        ctypes.byref(serial), ctypes.byref(max_comp_len),
                                        ctypes.byref(file_sys_flags), fs_name_buf,
                                        ctypes.sizeof(fs_name_buf))
    if rc:
        return serial.value
    return None

class ScanWorker(QThread):
    progress = Signal(int, int)
    finished = Signal(int, int)

    def __init__(self, db: Database, root_id: int, root_path: str, follow_symlinks: bool,
                 skip_hidden: bool, ignore_patterns):
        super().__init__()
        self.db = db
        self.root_id = root_id
        self.root_path = Path(root_path)
        self.follow_symlinks = follow_symlinks
        self.skip_hidden = skip_hidden
        self.ignore_patterns = [p.strip() for p in ignore_patterns if p.strip()]
        self._pause = False
        self._cancel = False

    def run(self):
        total_files = 0
        total_bytes = 0
        batch = []
        try:
            usage = shutil.disk_usage(self.root_path)
            capacity = usage.total
            free = usage.free
        except Exception:
            capacity = None
            free = None
        for dirpath, dirnames, filenames in os.walk(self.root_path, followlinks=self.follow_symlinks):
            if self._cancel:
                break
            if self.skip_hidden:
                dirnames[:] = [d for d in dirnames if not d.startswith('.')]
                filenames = [f for f in filenames if not f.startswith('.')]
            for f in filenames:
                if self._cancel:
                    break
                if any(fnmatch.fnmatch(f, pat) for pat in self.ignore_patterns):
                    continue
                try:
                    full_path = Path(dirpath) / f
                    stat = full_path.stat()
                    rel_path = os.path.relpath(full_path, self.root_path)
                    batch.append((self.root_id, f, rel_path, stat.st_size, int(stat.st_mtime)))
                    total_files += 1
                    total_bytes += stat.st_size
                    if len(batch) >= 1000:
                        self.db.insert_files_batch(batch)
                        batch.clear()
                    self.progress.emit(total_files, total_bytes)
                    while self._pause and not self._cancel:
                        self.msleep(200)
                except Exception as e:
                    logger.error("Error scanning %s: %s", full_path, e)
        if batch:
            self.db.insert_files_batch(batch)
        self.db.update_root_scan(self.root_id, capacity, free)
        self.finished.emit(total_files, total_bytes)

    def pause(self):
        self._pause = True

    def resume(self):
        self._pause = False

    def cancel(self):
        self._cancel = True

class DashboardTab(QWidget):
    def __init__(self, db: Database):
        super().__init__()
        self.db = db
        self.layout = QVBoxLayout(self)
        self.label = QLabel()
        self.layout.addWidget(self.label)
        self.refresh()

    def refresh(self):
        roots = self.db.list_roots()
        total_files = self.db.conn.execute("SELECT COUNT(*) FROM files").fetchone()[0]
        total_bytes = self.db.conn.execute("SELECT COALESCE(SUM(size),0) FROM files").fetchone()[0]
        stats = [f"Roots: {len(roots)}", f"Files: {total_files}", f"Bytes: {total_bytes}"]
        lines = []
        for r in roots:
            last_scan = r['last_scan_ts']
            if last_scan:
                dt = datetime.fromtimestamp(last_scan).strftime('%Y-%m-%d %H:%M')
            else:
                dt = 'never'
            lines.append(f"{r['alias']}: last scan {dt}")
        self.label.setText("\n".join(stats + lines))

class SearchTab(QWidget):
    def __init__(self, db: Database):
        super().__init__()
        self.db = db
        layout = QVBoxLayout(self)
        form = QHBoxLayout()
        self.name_edit = QLineEdit()
        self.min_size = QSpinBox(); self.min_size.setMaximum(10**9)
        self.max_size = QSpinBox(); self.max_size.setMaximum(10**9)
        self.ext_edit = QLineEdit(); self.ext_edit.setPlaceholderText('ext')
        self.search_btn = QPushButton('Search')
        form.addWidget(QLabel('Name')); form.addWidget(self.name_edit)
        form.addWidget(QLabel('Min size')); form.addWidget(self.min_size)
        form.addWidget(QLabel('Max size')); form.addWidget(self.max_size)
        form.addWidget(QLabel('Ext')); form.addWidget(self.ext_edit)
        form.addWidget(self.search_btn)
        layout.addLayout(form)
        self.table = QTableWidget(0,5)
        self.table.setHorizontalHeaderLabels(['Name','Size','Modified','Full Path','Alias'])
        self.table.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        layout.addWidget(self.table)
        self.search_btn.clicked.connect(self.do_search)
        self.table.itemDoubleClicked.connect(self.open_item)

    def do_search(self):
        name = self.name_edit.text()
        min_size = self.min_size.value() or None
        max_size = self.max_size.value() or None
        ext = self.ext_edit.text()
        rows = self.db.search_files(name, min_size, max_size, ext, 100, 0)
        self.table.setRowCount(0)
        for row in rows:
            r = self.table.rowCount(); self.table.insertRow(r)
            self.table.setItem(r,0,QTableWidgetItem(row['file_name']))
            self.table.setItem(r,1,QTableWidgetItem(str(row['size'])))
            dt = datetime.fromtimestamp(row['modified_time']).strftime('%Y-%m-%d %H:%M')
            self.table.setItem(r,2,QTableWidgetItem(dt))
            self.table.setItem(r,3,QTableWidgetItem(row['full_path']))
            self.table.setItem(r,4,QTableWidgetItem(row['alias']))

    def open_item(self, item):
        row = item.row()
        path = self.table.item(row, 3).text()
        try:
            os.startfile(path)
        except Exception as e:
            QMessageBox.warning(self, 'Open File', f'Cannot open file: {e}')

class ReportsTab(QWidget):
    def __init__(self, db: Database):
        super().__init__()
        self.db = db
        layout = QVBoxLayout(self)
        self.figure = Figure(figsize=(5,4))
        self.canvas = FigureCanvas(self.figure)
        layout.addWidget(self.canvas)
        self.refresh()

    def refresh(self):
        self.figure.clear()
        ax = self.figure.add_subplot(111)
        roots = self.db.list_roots()
        aliases = [r['alias'] for r in roots]
        used = []
        free = []
        for r in roots:
            used_bytes = self.db.conn.execute("SELECT COALESCE(SUM(size),0) FROM files WHERE root_id=?", (r['id'],)).fetchone()[0]
            if r['scanned_total_capacity_bytes']:
                free_bytes = r['scanned_total_capacity_bytes'] - used_bytes
            else:
                free_bytes = 0
            used.append(used_bytes/1e9)
            free.append(max(free_bytes,0)/1e9)
        ax.barh(aliases, used, color='orange', label='Used')
        ax.barh(aliases, free, left=used, color='blue', label='Free')
        ax.set_xlabel('GB')
        ax.legend()
        self.canvas.draw()

class QueueTab(QWidget):
    def __init__(self, db: Database, settings):
        super().__init__()
        self.db = db
        self.settings = settings
        self.layout = QVBoxLayout(self)
        form = QHBoxLayout()
        self.path_edit = QLineEdit()
        self.alias_edit = QLineEdit()
        browse_btn = QPushButton('Browse')
        add_btn = QPushButton('Add to Queue')
        form.addWidget(self.path_edit)
        form.addWidget(browse_btn)
        form.addWidget(QLabel('Alias'))
        form.addWidget(self.alias_edit)
        form.addWidget(add_btn)
        self.layout.addLayout(form)
        self.list = QListWidget()
        self.layout.addWidget(self.list)
        self.start_btn = QPushButton('Start')
        self.pause_btn = QPushButton('Pause')
        self.resume_btn = QPushButton('Resume')
        self.cancel_btn = QPushButton('Cancel')
        btns = QHBoxLayout(); btns.addWidget(self.start_btn); btns.addWidget(self.pause_btn); btns.addWidget(self.resume_btn); btns.addWidget(self.cancel_btn)
        self.layout.addLayout(btns)
        browse_btn.clicked.connect(self.browse)
        add_btn.clicked.connect(self.add_queue)
        self.start_btn.clicked.connect(self.start)
        self.pause_btn.clicked.connect(self.pause)
        self.resume_btn.clicked.connect(self.resume)
        self.cancel_btn.clicked.connect(self.cancel)
        self.current_worker = None

    def browse(self):
        path = QFileDialog.getExistingDirectory(self, 'Select Root')
        if path:
            self.path_edit.setText(path)

    def add_queue(self):
        path = self.path_edit.text()
        alias = self.alias_edit.text()
        if not path or not alias:
            return
        if self.db.get_root_by_alias(alias):
            QMessageBox.warning(self, 'Alias exists', 'Alias already used')
            return
        item = QListWidgetItem(f"{alias}: {path}")
        item.setData(Qt.UserRole, (path, alias))
        self.list.addItem(item)
        self.path_edit.clear(); self.alias_edit.clear()

    def start(self):
        if self.current_worker or self.list.count()==0:
            return
        item = self.list.takeItem(0)
        path, alias = item.data(Qt.UserRole)
        volume_serial = get_volume_serial(path)
        root_id = self.db.add_root(alias, path, volume_serial)
        self.db.conn.execute("DELETE FROM files WHERE root_id=?", (root_id,))
        self.db.conn.commit()
        opts = self.settings.get_options()
        self.current_worker = ScanWorker(self.db, root_id, path, opts['follow_symlinks'], opts['skip_hidden'], opts['ignore_patterns'])
        self.current_worker.finished.connect(self.worker_finished)
        self.current_worker.start()

    def worker_finished(self, files, bytes_):
        logger.info("Scan finished: %s files, %s bytes", files, bytes_)
        self.current_worker = None
        self.start()

    def pause(self):
        if self.current_worker:
            self.current_worker.pause()

    def resume(self):
        if self.current_worker:
            self.current_worker.resume()

    def cancel(self):
        if self.current_worker:
            self.current_worker.cancel()
            self.current_worker = None

class SettingsTab(QWidget):
    def __init__(self, db: Database):
        super().__init__()
        self.db = db
        layout = QVBoxLayout(self)
        form = QFormLayout()
        self.follow_symlinks = QCheckBox()
        self.skip_hidden = QCheckBox(); self.skip_hidden.setChecked(True)
        self.ignore_edit = QLineEdit()
        form.addRow('Follow symlinks', self.follow_symlinks)
        form.addRow('Skip hidden', self.skip_hidden)
        form.addRow('Ignore patterns', self.ignore_edit)
        layout.addLayout(form)
        schedule_layout = QHBoxLayout()
        self.schedule_check = QCheckBox('Enable daily scan')
        self.time_edit = QTimeEdit()
        self.time_edit.setDisplayFormat('HH:mm')
        schedule_layout.addWidget(self.schedule_check)
        schedule_layout.addWidget(self.time_edit)
        layout.addLayout(schedule_layout)
        btn = QPushButton('Save')
        layout.addWidget(btn)
        btn.clicked.connect(self.save)
        self.load()

    def get_options(self):
        return {
            'follow_symlinks': self.follow_symlinks.isChecked(),
            'skip_hidden': self.skip_hidden.isChecked(),
            'ignore_patterns': self.ignore_edit.text().split(',')
        }

    def load(self):
        opts = self.db.get_config('settings', {
            'follow_symlinks': False,
            'skip_hidden': True,
            'ignore_patterns': [],
            'schedule': {'enabled': False, 'time': '00:00'}
        })
        self.follow_symlinks.setChecked(opts.get('follow_symlinks', False))
        self.skip_hidden.setChecked(opts.get('skip_hidden', True))
        self.ignore_edit.setText(','.join(opts.get('ignore_patterns', [])))
        sched = opts.get('schedule', {})
        self.schedule_check.setChecked(sched.get('enabled', False))
        h,m = map(int, sched.get('time','00:00').split(':'))
        self.time_edit.setTime(time(h,m))

    def save(self):
        opts = {
            'follow_symlinks': self.follow_symlinks.isChecked(),
            'skip_hidden': self.skip_hidden.isChecked(),
            'ignore_patterns': self.ignore_edit.text().split(','),
            'schedule': {
                'enabled': self.schedule_check.isChecked(),
                'time': self.time_edit.time().toString('HH:mm')
            }
        }
        self.db.set_config('settings', opts)
        QMessageBox.information(self, 'Saved', 'Settings saved')

class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.db = Database(DB_PATH)
        self.setWindowTitle('DriveCatalogue V5')
        tabs = QTabWidget()
        self.dashboard = DashboardTab(self.db)
        self.search = SearchTab(self.db)
        self.settings = SettingsTab(self.db)
        self.reports = ReportsTab(self.db)
        self.queue = QueueTab(self.db, self.settings)
        tabs.addTab(self.dashboard, 'Dashboard')
        tabs.addTab(self.search, 'Search')
        tabs.addTab(self.reports, 'Reports')
        tabs.addTab(self.queue, 'Queue')
        tabs.addTab(self.settings, 'Settings')
        self.setCentralWidget(tabs)
        self.setup_menu()
        self.schedule_timer = QTimer()
        self.schedule_timer.timeout.connect(self.check_schedule)
        self.schedule_timer.start(60_000)

    def setup_menu(self):
        menubar = self.menuBar()
        file_menu = menubar.addMenu('File')
        backup = QAction('Backup Database', self)
        export_csv = QAction('Export CSV', self)
        export_json = QAction('Export JSON', self)
        backup.triggered.connect(self.backup_db)
        export_csv.triggered.connect(self.export_csv)
        export_json.triggered.connect(self.export_json)
        file_menu.addAction(backup)
        file_menu.addAction(export_csv)
        file_menu.addAction(export_json)

    def backup_db(self):
        path = self.db.backup_db()
        QMessageBox.information(self, 'Backup', f'Backup created at {path}')

    def export_csv(self):
        path, _ = QFileDialog.getSaveFileName(self, 'Export CSV', str(APP_DIR), 'CSV Files (*.csv)')
        if path:
            self.db.export_csv(path)

    def export_json(self):
        path, _ = QFileDialog.getSaveFileName(self, 'Export JSON', str(APP_DIR), 'JSON Files (*.json)')
        if path:
            self.db.export_json(path)

    def check_schedule(self):
        opts = self.db.get_config('settings', {})
        sched = opts.get('schedule', {})
        if not sched.get('enabled'):
            return
        now = datetime.now().strftime('%H:%M')
        if now == sched.get('time'):
            roots = self.db.list_roots()
            for r in roots:
                item = QListWidgetItem(f"{r['alias']}: {r['root_path']}")
                item.setData(Qt.UserRole, (r['root_path'], r['alias']))
                self.queue.list.addItem(item)
            self.queue.start()


def main():
    app = QApplication(sys.argv)
    win = MainWindow()
    win.resize(1000, 600)
    win.show()
    sys.exit(app.exec())

if __name__ == '__main__':
    try:
        main()
    except Exception:
        logger.exception('Unhandled exception')
