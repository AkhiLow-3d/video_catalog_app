import os
import sys
import shutil
import sqlite3
import hashlib
import subprocess
from pathlib import Path
from typing import Optional

import cv2
from PySide6.QtCore import Qt, QSize
from PySide6.QtGui import QAction, QIcon, QPixmap
from PySide6.QtWidgets import (
    QAbstractItemView,
    QApplication,
    QFileDialog,
    QFormLayout,
    QGridLayout,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QListWidget,
    QListWidgetItem,
    QMainWindow,
    QMenu,
    QMessageBox,
    QPushButton,
    QSplitter,
    QTextEdit,
    QToolBar,
    QVBoxLayout,
    QWidget,
)

APP_NAME = "Video Catalog"
BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "video_catalog_data"
THUMB_DIR = DATA_DIR / "thumbnails"
DB_PATH = DATA_DIR / "catalog.db"

VIDEO_EXTENSIONS = {
    ".mp4", ".mov", ".avi", ".mkv", ".webm", ".wmv", ".m4v", ".flv", ".mpeg", ".mpg"
}
THUMB_SIZE = 220

PRESET_COLLECTIONS = [
    "モデリング",
    "アニメーション",
    "マテリアル",
    "ライティング",
    "背景",
    "キャラ",
    "資料",
    "チュートリアル",
]

PRESET_TAGS = [
    "wood",
    "metal",
    "stone",
    "cloth",
    "grunge",
    "clean",
    "stylized",
    "realistic",
    "blender",
    "substance",
    "2D風",
    "背景",
]


def parse_csv_text(text: str) -> list[str]:
    return [p.strip() for p in text.replace("、", ",").split(",") if p.strip()]


def normalize_single_value(text: str) -> str:
    parts = parse_csv_text(text)
    return parts[0] if parts else ""


def normalize_csv_text(text: str) -> str:
    parts = parse_csv_text(text)
    seen: list[str] = []
    for p in parts:
        if p not in seen:
            seen.append(p)
    return ", ".join(seen)


def merge_csv_values(base_text: str, add_text: str) -> str:
    merged = parse_csv_text(base_text) + parse_csv_text(add_text)
    seen: list[str] = []
    for value in merged:
        if value not in seen:
            seen.append(value)
    return ", ".join(seen)


def common_single_value(values: list[str]) -> str:
    normalized = [normalize_single_value(value) for value in values if normalize_single_value(value)]
    if not normalized:
        return ""
    first = normalized[0]
    return first if all(value == first for value in normalized) else ""


def common_csv_values(values: list[str]) -> list[str]:
    parsed = [set(parse_csv_text(value)) for value in values]
    parsed = [value_set for value_set in parsed if value_set]
    if not parsed:
        return []
    common = set.intersection(*parsed)
    return sorted(common)


def ensure_thumbnail(video_path: Path) -> Optional[Path]:
    THUMB_DIR.mkdir(parents=True, exist_ok=True)

    key = hashlib.sha1(str(video_path).encode("utf-8", errors="ignore")).hexdigest()
    thumb_path = THUMB_DIR / f"{key}.jpg"

    try:
        stat = video_path.stat()
        if thumb_path.exists() and thumb_path.stat().st_mtime >= stat.st_mtime:
            return thumb_path
    except Exception:
        pass

    cap = cv2.VideoCapture(str(video_path))
    if not cap.isOpened():
        return None

    try:
        frame_count = int(cap.get(cv2.CAP_PROP_FRAME_COUNT) or 0)
        target_frame = max(0, min(frame_count - 1, int(frame_count * 0.1))) if frame_count > 0 else 0
        cap.set(cv2.CAP_PROP_POS_FRAMES, target_frame)
        success, frame = cap.read()

        if not success or frame is None:
            cap.set(cv2.CAP_PROP_POS_FRAMES, 0)
            success, frame = cap.read()

        if not success or frame is None:
            return None

        height, width = frame.shape[:2]
        scale = THUMB_SIZE / max(width, height)
        new_w = max(1, int(width * scale))
        new_h = max(1, int(height * scale))
        resized = cv2.resize(frame, (new_w, new_h), interpolation=cv2.INTER_AREA)
        cv2.imwrite(str(thumb_path), resized)
        return thumb_path
    finally:
        cap.release()


class Database:
    def __init__(self, db_path: Path) -> None:
        self.db_path = db_path
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        THUMB_DIR.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(str(self.db_path))
        self.conn.row_factory = sqlite3.Row
        self._create_tables()
        self._ensure_columns()

    def _create_tables(self) -> None:
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS libraries (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                root_path TEXT NOT NULL UNIQUE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS videos (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                library_id INTEGER NOT NULL,
                title TEXT NOT NULL,
                filename TEXT NOT NULL,
                absolute_path TEXT NOT NULL UNIQUE,
                relative_path TEXT,
                thumbnail_path TEXT,
                file_size INTEGER,
                modified_time REAL,
                tags TEXT DEFAULT '',
                collections TEXT DEFAULT '',
                note TEXT DEFAULT '',
                is_missing INTEGER DEFAULT 0,
                is_ignored INTEGER DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (library_id) REFERENCES libraries(id)
            )
            """
        )
        self.conn.commit()

    def _ensure_columns(self) -> None:
        columns = {row["name"] for row in self.conn.execute("PRAGMA table_info(videos)").fetchall()}
        if "is_ignored" not in columns:
            self.conn.execute("ALTER TABLE videos ADD COLUMN is_ignored INTEGER DEFAULT 0")
            self.conn.commit()

    def reset_all_data(self) -> None:
        self.conn.close()
        if self.db_path.exists():
            self.db_path.unlink()
        if THUMB_DIR.exists():
            shutil.rmtree(THUMB_DIR, ignore_errors=True)
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        THUMB_DIR.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(str(self.db_path))
        self.conn.row_factory = sqlite3.Row
        self._create_tables()
        self._ensure_columns()

    def get_or_create_library(self, root_path: Path) -> int:
        row = self.conn.execute(
            "SELECT id FROM libraries WHERE root_path = ?",
            (str(root_path),),
        ).fetchone()
        if row:
            return int(row["id"])

        cur = self.conn.execute(
            "INSERT INTO libraries (root_path) VALUES (?)",
            (str(root_path),),
        )
        self.conn.commit()
        return int(cur.lastrowid)

    def get_libraries(self):
        return self.conn.execute(
            "SELECT * FROM libraries ORDER BY created_at DESC"
        ).fetchall()

    def find_video_by_path(self, absolute_path: Path):
        return self.conn.execute(
            "SELECT * FROM videos WHERE absolute_path = ?",
            (str(absolute_path),),
        ).fetchone()

    def upsert_video(
        self,
        library_id: int,
        title: str,
        filename: str,
        absolute_path: Path,
        relative_path: Optional[str],
        thumbnail_path: Optional[Path],
        file_size: int,
        modified_time: float,
    ) -> None:
        existing = self.find_video_by_path(absolute_path)

        if existing:
            self.conn.execute(
                """
                UPDATE videos
                SET title = ?,
                    filename = ?,
                    relative_path = ?,
                    thumbnail_path = ?,
                    file_size = ?,
                    modified_time = ?,
                    is_missing = 0,
                    updated_at = CURRENT_TIMESTAMP
                WHERE absolute_path = ?
                """,
                (
                    title,
                    filename,
                    relative_path,
                    str(thumbnail_path) if thumbnail_path else None,
                    file_size,
                    modified_time,
                    str(absolute_path),
                ),
            )
        else:
            self.conn.execute(
                """
                INSERT INTO videos (
                    library_id, title, filename, absolute_path, relative_path,
                    thumbnail_path, file_size, modified_time, is_missing, is_ignored
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0, 0)
                """,
                (
                    library_id,
                    title,
                    filename,
                    str(absolute_path),
                    relative_path,
                    str(thumbnail_path) if thumbnail_path else None,
                    file_size,
                    modified_time,
                ),
            )
        self.conn.commit()

    def mark_missing_for_library(self, library_id: int) -> None:
        self.conn.execute(
            "UPDATE videos SET is_missing = 1, updated_at = CURRENT_TIMESTAMP WHERE library_id = ?",
            (library_id,),
        )
        self.conn.commit()

    def get_videos(
        self,
        search_text: str = "",
        missing_only: bool = False,
        include_ignored: bool = False,
    ):
        query = "SELECT * FROM videos"
        clauses = []
        params = []

        if not include_ignored:
            clauses.append("is_ignored = 0")

        if search_text.strip():
            pattern = f"%{search_text.strip()}%"
            clauses.append(
                "(title LIKE ? OR filename LIKE ? OR tags LIKE ? OR collections LIKE ? OR note LIKE ?)"
            )
            params.extend([pattern, pattern, pattern, pattern, pattern])

        if missing_only:
            clauses.append("is_missing = 1")

        if clauses:
            query += " WHERE " + " AND ".join(clauses)

        query += " ORDER BY is_missing DESC, updated_at DESC, title COLLATE NOCASE ASC"
        return self.conn.execute(query, params).fetchall()

    def get_video(self, video_id: int):
        return self.conn.execute(
            "SELECT * FROM videos WHERE id = ?",
            (video_id,),
        ).fetchone()

    def update_video_metadata(
        self,
        video_id: int,
        title: str,
        tags: str,
        collections: str,
        note: str,
    ) -> None:
        self.conn.execute(
            """
            UPDATE videos
            SET title = ?, tags = ?, collections = ?, note = ?, updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
            """,
            (title, tags, collections, note, video_id),
        )
        self.conn.commit()

    def set_ignored(self, video_ids: list[int], ignored: bool = True) -> None:
        for video_id in video_ids:
            self.conn.execute(
                "UPDATE videos SET is_ignored = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
                (1 if ignored else 0, video_id),
            )
        self.conn.commit()

    def close(self) -> None:
        self.conn.close()


class MainWindow(QMainWindow):
    def __init__(self) -> None:
        super().__init__()
        self.db = Database(DB_PATH)
        self.current_video_id: Optional[int] = None
        self.collection_buttons: dict[str, QPushButton] = {}
        self.tag_buttons: dict[str, QPushButton] = {}
        self._updating_fields = False

        self.setWindowTitle(APP_NAME)
        self.resize(1500, 920)

        self._build_ui()
        self.reload_list()

    def _build_ui(self) -> None:
        self._build_toolbar()

        central = QWidget()
        self.setCentralWidget(central)

        root_layout = QVBoxLayout(central)
        root_layout.setContentsMargins(8, 8, 8, 8)
        root_layout.setSpacing(8)

        top_bar = QHBoxLayout()
        root_layout.addLayout(top_bar)

        self.search_input = QLineEdit()
        self.search_input.setPlaceholderText("検索: タイトル / タグ / コレクション / メモ")
        self.search_input.textChanged.connect(self.reload_list)
        top_bar.addWidget(self.search_input, 1)

        self.missing_filter_btn = QPushButton("リンク切れのみ: OFF")
        self.missing_filter_btn.setCheckable(True)
        self.missing_filter_btn.toggled.connect(self._on_toggle_missing_filter)
        top_bar.addWidget(self.missing_filter_btn)

        self.result_label = QLabel("0件")
        top_bar.addWidget(self.result_label)

        side_splitter = QSplitter()
        side_splitter.setOrientation(Qt.Horizontal)
        root_layout.addWidget(side_splitter, 1)

        left_panel = QWidget()
        left_layout = QVBoxLayout(left_panel)
        left_layout.setContentsMargins(0, 0, 0, 0)
        left_layout.setSpacing(8)

        left_layout.addWidget(QLabel("コレクション一覧"))
        self.collection_list_widget = QListWidget()
        self.collection_list_widget.itemClicked.connect(self.on_collection_list_clicked)
        left_layout.addWidget(self.collection_list_widget)

        left_layout.addWidget(QLabel("タグ一覧"))
        self.tag_list_widget = QListWidget()
        self.tag_list_widget.itemClicked.connect(self.on_tag_list_clicked)
        left_layout.addWidget(self.tag_list_widget)

        filter_row = QHBoxLayout()
        left_layout.addLayout(filter_row)

        self.unclassified_filter_btn = QPushButton("未分類のみ: OFF")
        self.unclassified_filter_btn.setCheckable(True)
        self.unclassified_filter_btn.toggled.connect(self._on_toggle_unclassified_filter)
        filter_row.addWidget(self.unclassified_filter_btn)

        self.clear_search_btn = QPushButton("絞り込み解除")
        self.clear_search_btn.clicked.connect(self.clear_all_filters)
        filter_row.addWidget(self.clear_search_btn)

        side_splitter.addWidget(left_panel)

        main_splitter = QSplitter()
        main_splitter.setOrientation(Qt.Horizontal)
        side_splitter.addWidget(main_splitter)
        side_splitter.setSizes([260, 1240])

        self.list_widget = QListWidget()
        self.list_widget.setSelectionMode(QAbstractItemView.ExtendedSelection)
        self.list_widget.setViewMode(QListWidget.IconMode)
        self.list_widget.setIconSize(QSize(THUMB_SIZE, THUMB_SIZE))
        self.list_widget.setResizeMode(QListWidget.Adjust)
        self.list_widget.setMovement(QListWidget.Static)
        self.list_widget.setSpacing(12)
        self.list_widget.setWordWrap(True)
        self.list_widget.setUniformItemSizes(False)
        self.list_widget.itemSelectionChanged.connect(self._on_selection_changed)
        self.list_widget.itemDoubleClicked.connect(self.open_selected_video)
        self.list_widget.setContextMenuPolicy(Qt.CustomContextMenu)
        self.list_widget.customContextMenuRequested.connect(self.show_list_context_menu)
        main_splitter.addWidget(self.list_widget)

        detail_panel = QWidget()
        detail_layout = QVBoxLayout(detail_panel)
        main_splitter.addWidget(detail_panel)
        main_splitter.setSizes([920, 500])

        form = QFormLayout()
        detail_layout.addLayout(form)

        self.title_edit = QLineEdit()
        form.addRow("タイトル", self.title_edit)

        self.path_label = QLabel("-")
        self.path_label.setWordWrap(True)
        form.addRow("パス", self.path_label)

        self.info_label = QLabel("-")
        self.info_label.setWordWrap(True)
        form.addRow("情報", self.info_label)

        collection_box = QWidget()
        collection_layout = QVBoxLayout(collection_box)
        collection_layout.setContentsMargins(0, 0, 0, 0)
        collection_layout.setSpacing(6)

        collection_grid = QGridLayout()
        collection_grid.setContentsMargins(0, 0, 0, 0)
        collection_grid.setHorizontalSpacing(6)
        collection_grid.setVerticalSpacing(6)
        collection_layout.addLayout(collection_grid)

        for index, name in enumerate(PRESET_COLLECTIONS):
            btn = QPushButton(name)
            btn.setCheckable(True)
            btn.clicked.connect(self._on_collection_button_clicked)
            self.collection_buttons[name] = btn
            collection_grid.addWidget(btn, index // 2, index % 2)

        self.collections_edit = QLineEdit()
        self.collections_edit.setPlaceholderText("主カテゴリを1つ。必要なら手入力も可")
        self.collections_edit.textEdited.connect(self.sync_collection_buttons_from_text)
        collection_layout.addWidget(self.collections_edit)
        form.addRow("コレクション", collection_box)

        tag_box = QWidget()
        tag_layout = QVBoxLayout(tag_box)
        tag_layout.setContentsMargins(0, 0, 0, 0)
        tag_layout.setSpacing(6)

        tag_grid = QGridLayout()
        tag_grid.setContentsMargins(0, 0, 0, 0)
        tag_grid.setHorizontalSpacing(6)
        tag_grid.setVerticalSpacing(6)
        tag_layout.addLayout(tag_grid)

        for index, name in enumerate(PRESET_TAGS):
            btn = QPushButton(name)
            btn.setCheckable(True)
            btn.clicked.connect(self._on_tag_button_clicked)
            self.tag_buttons[name] = btn
            tag_grid.addWidget(btn, index // 3, index % 3)

        self.tags_edit = QLineEdit()
        self.tags_edit.setPlaceholderText("特徴タグ。カンマ区切りで手入力も可")
        self.tags_edit.textEdited.connect(self.sync_tag_buttons_from_text)
        tag_layout.addWidget(self.tags_edit)
        form.addRow("タグ", tag_box)

        self.note_edit = QTextEdit()
        self.note_edit.setPlaceholderText("メモ")
        form.addRow("メモ", self.note_edit)

        button_row = QHBoxLayout()
        detail_layout.addLayout(button_row)

        self.open_btn = QPushButton("既定アプリで開く")
        self.open_btn.clicked.connect(self.open_selected_video)
        button_row.addWidget(self.open_btn)

        self.save_btn = QPushButton("保存")
        self.save_btn.clicked.connect(self.save_current_video)
        button_row.addWidget(self.save_btn)

        self.bulk_apply_btn = QPushButton("選択中に一括適用")
        self.bulk_apply_btn.clicked.connect(self.apply_to_selected_videos)
        button_row.addWidget(self.bulk_apply_btn)

        detail_layout.addStretch(1)

    def _build_toolbar(self) -> None:
        toolbar = QToolBar("Main Toolbar")
        self.addToolBar(toolbar)

        add_folder_action = QAction("フォルダ追加", self)
        add_folder_action.triggered.connect(self.add_library_folder)
        toolbar.addAction(add_folder_action)

        rescan_action = QAction("再スキャン", self)
        rescan_action.triggered.connect(self.rescan_all_libraries)
        toolbar.addAction(rescan_action)

        reload_action = QAction("再読込", self)
        reload_action.triggered.connect(self.reload_list)
        toolbar.addAction(reload_action)

        ignore_action = QAction("選択中を無視", self)
        ignore_action.triggered.connect(self.ignore_selected_videos)
        toolbar.addAction(ignore_action)

        reset_action = QAction("データ初期化", self)
        reset_action.triggered.connect(self.reset_all_data)
        toolbar.addAction(reset_action)

    def _on_toggle_missing_filter(self, checked: bool) -> None:
        self.missing_filter_btn.setText(f"リンク切れのみ: {'ON' if checked else 'OFF'}")
        self.reload_list()

    def _on_toggle_unclassified_filter(self, checked: bool) -> None:
        self.unclassified_filter_btn.setText(f"未分類のみ: {'ON' if checked else 'OFF'}")
        self.reload_list()

    def clear_all_filters(self) -> None:
        self.search_input.clear()
        self.missing_filter_btn.setChecked(False)
        self.unclassified_filter_btn.setChecked(False)
        self.collection_list_widget.clearSelection()
        self.tag_list_widget.clearSelection()
        self.reload_list()

    def add_library_folder(self) -> None:
        folder = QFileDialog.getExistingDirectory(self, "動画フォルダを選択")
        if not folder:
            return

        root = Path(folder)
        library_id = self.db.get_or_create_library(root)
        self.scan_library(root, library_id)
        self.reload_list()
        QMessageBox.information(self, APP_NAME, f"スキャン完了: {root}")

    def rescan_all_libraries(self) -> None:
        libraries = self.db.get_libraries()
        if not libraries:
            QMessageBox.information(self, APP_NAME, "先にフォルダを追加してください。")
            return

        for row in libraries:
            root = Path(row["root_path"])
            library_id = int(row["id"])
            self.scan_library(root, library_id)

        self.reload_list()
        QMessageBox.information(self, APP_NAME, "全ライブラリを再スキャンしました。")

    def reset_all_data(self) -> None:
        reply = QMessageBox.question(
            self,
            APP_NAME,
            "保存済みデータとサムネイルを初期化しますか？\n元の動画ファイルは削除されません。",
            QMessageBox.Yes | QMessageBox.No,
            QMessageBox.No,
        )
        if reply != QMessageBox.Yes:
            return

        self.db.reset_all_data()
        self.clear_all_filters()
        self.clear_detail()
        self.reload_list()
        QMessageBox.information(self, APP_NAME, "データを初期化しました。")

    def scan_library(self, root: Path, library_id: int) -> None:
        self.db.mark_missing_for_library(library_id)

        if not root.exists():
            return

        for path in root.rglob("*"):
            if not path.is_file():
                continue
            if path.suffix.lower() not in VIDEO_EXTENSIONS:
                continue

            try:
                stat = path.stat()
                rel = str(path.relative_to(root))
            except Exception:
                continue

            existing = self.db.find_video_by_path(path)
            if existing and int(existing["is_ignored"] or 0) == 1:
                self.db.conn.execute(
                    "UPDATE videos SET is_missing = 0, modified_time = ?, file_size = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
                    (stat.st_mtime, stat.st_size, int(existing["id"])),
                )
                self.db.conn.commit()
                continue

            thumb_path = ensure_thumbnail(path)
            self.db.upsert_video(
                library_id=library_id,
                title=path.stem,
                filename=path.name,
                absolute_path=path,
                relative_path=rel,
                thumbnail_path=thumb_path,
                file_size=stat.st_size,
                modified_time=stat.st_mtime,
            )

    def reload_list(self) -> None:
        self.list_widget.clear()

        selected_collection_item = self.collection_list_widget.currentItem()
        selected_tag_item = self.tag_list_widget.currentItem()

        selected_collection = selected_collection_item.text() if selected_collection_item else ""
        if selected_collection == "すべて":
            selected_collection = ""

        selected_tag = selected_tag_item.text() if selected_tag_item else ""
        if selected_tag == "すべて":
            selected_tag = ""

        rows = self.db.get_videos(
            search_text=self.search_input.text(),
            missing_only=self.missing_filter_btn.isChecked(),
        )

        if self.unclassified_filter_btn.isChecked():
            rows = [
                row for row in rows
                if not (row["collections"] or "").strip() and not (row["tags"] or "").strip()
            ]

        if selected_collection == "未分類":
            rows = [row for row in rows if not (row["collections"] or "").strip()]
        elif selected_collection:
            rows = [
                row for row in rows
                if (row["collections"] or "").strip() == selected_collection
            ]

        if selected_tag:
            rows = [
                row for row in rows
                if selected_tag in parse_csv_text(row["tags"] or "")
            ]

        for row in rows:
            title = row["title"] or row["filename"]
            tags = (row["tags"] or "").strip()
            collections = (row["collections"] or "").strip()
            missing = bool(row["is_missing"])

            tooltip_lines = [title]
            if collections:
                tooltip_lines.append(f"コレクション: {collections}")
            if tags:
                tooltip_lines.append(f"タグ: {tags}")
            if missing:
                tooltip_lines.append("[リンク切れ]")

            item = QListWidgetItem(title)
            item.setToolTip("\n".join(tooltip_lines))
            item.setData(Qt.UserRole, int(row["id"]))
            item.setSizeHint(QSize(220, 240))

            thumb_path = row["thumbnail_path"]
            if thumb_path and Path(thumb_path).exists():
                pix = QPixmap(str(thumb_path))
                item.setIcon(QIcon(pix))
            else:
                placeholder = QPixmap(THUMB_SIZE, THUMB_SIZE)
                placeholder.fill(Qt.darkGray)
                item.setIcon(QIcon(placeholder))

            self.list_widget.addItem(item)

        self.result_label.setText(f"{len(rows)}件")
        self.refresh_filter_lists()

        if self.list_widget.count() > 0:
            self.list_widget.setCurrentRow(0)
        else:
            self.clear_detail()

    def refresh_filter_lists(self) -> None:
        current_collection = (
            self.collection_list_widget.currentItem().text()
            if self.collection_list_widget.currentItem()
            else ""
        )
        current_tag = (
            self.tag_list_widget.currentItem().text()
            if self.tag_list_widget.currentItem()
            else ""
        )

        all_rows = self.db.get_videos()
        collections = sorted(
            {(row["collections"] or "").strip() for row in all_rows if (row["collections"] or "").strip()}
        )
        tags = sorted(
            {tag for row in all_rows for tag in parse_csv_text(row["tags"] or "")}
        )

        self.collection_list_widget.blockSignals(True)
        self.tag_list_widget.blockSignals(True)

        self.collection_list_widget.clear()
        self.tag_list_widget.clear()

        self.collection_list_widget.addItem("すべて")
        self.collection_list_widget.addItem("未分類")
        self.collection_list_widget.addItems(collections)

        self.tag_list_widget.addItem("すべて")
        self.tag_list_widget.addItems(tags)

        if current_collection:
            matches = self.collection_list_widget.findItems(current_collection, Qt.MatchExactly)
            if matches:
                self.collection_list_widget.setCurrentItem(matches[0])

        if current_tag:
            matches = self.tag_list_widget.findItems(current_tag, Qt.MatchExactly)
            if matches:
                self.tag_list_widget.setCurrentItem(matches[0])

        self.collection_list_widget.blockSignals(False)
        self.tag_list_widget.blockSignals(False)

    def on_collection_list_clicked(self, item: QListWidgetItem) -> None:
        self.tag_list_widget.clearSelection()
        self.reload_list()

    def on_tag_list_clicked(self, item: QListWidgetItem) -> None:
        self.collection_list_widget.clearSelection()
        self.reload_list()

    def clear_detail(self) -> None:
        self.current_video_id = None
        self.title_edit.clear()
        self.path_label.setText("-")
        self.info_label.setText("-")
        self.tags_edit.clear()
        self.collections_edit.clear()
        self.note_edit.clear()
        self._set_collection_buttons(set())
        self._set_tag_buttons(set())

    def _on_selection_changed(self) -> None:
        items = self.list_widget.selectedItems()
        if not items:
            self.clear_detail()
            return

        if len(items) > 1:
            self.current_video_id = None
            selected_ids = [int(item.data(Qt.UserRole)) for item in items]
            self.title_edit.setText("")
            self.path_label.setText(f"{len(items)}件選択中")
            self.info_label.setText(
                "複数選択中です。タグ / コレクション / メモを入力して『選択中に一括適用』を押してください。"
            )

            rows = [self.db.get_video(video_id) for video_id in selected_ids]
            rows = [row for row in rows if row is not None]

            common_collection = common_single_value([row["collections"] or "" for row in rows])
            common_tags = common_csv_values([row["tags"] or "" for row in rows])

            self._updating_fields = True
            self.collections_edit.setText(common_collection)
            self.tags_edit.setText(", ".join(common_tags))
            self.note_edit.clear()
            self._updating_fields = False

            self.sync_collection_buttons_from_text()
            self.sync_tag_buttons_from_text()
            return

        video_id = int(items[0].data(Qt.UserRole))
        row = self.db.get_video(video_id)
        if row is None:
            self.clear_detail()
            return

        self.current_video_id = video_id
        self.title_edit.setText(row["title"] or "")
        self.path_label.setText(row["absolute_path"] or "")

        size_mb = (row["file_size"] or 0) / (1024 * 1024)
        info = (
            f"ファイル名: {row['filename']}\n"
            f"サイズ: {size_mb:.2f} MB\n"
            f"状態: {'リンク切れ' if row['is_missing'] else '正常'}"
        )
        self.info_label.setText(info)

        self._updating_fields = True
        self.collections_edit.setText(row["collections"] or "")
        self.tags_edit.setText(row["tags"] or "")
        self.note_edit.setPlainText(row["note"] or "")
        self._updating_fields = False

        self.sync_collection_buttons_from_text()
        self.sync_tag_buttons_from_text()

    def _on_collection_button_clicked(self) -> None:
        clicked_button = self.sender()
        if not isinstance(clicked_button, QPushButton):
            return

        for button in self.collection_buttons.values():
            if button is not clicked_button:
                button.setChecked(False)

        if clicked_button.isChecked():
            self.collections_edit.setText(clicked_button.text())
        elif self.collections_edit.text().strip() == clicked_button.text():
            self.collections_edit.clear()

    def _on_tag_button_clicked(self) -> None:
        selected_tags = [name for name, button in self.tag_buttons.items() if button.isChecked()]
        custom_tags = [
            tag for tag in parse_csv_text(self.tags_edit.text())
            if tag not in self.tag_buttons
        ]
        merged = selected_tags + [tag for tag in custom_tags if tag not in selected_tags]

        self._updating_fields = True
        self.tags_edit.setText(", ".join(merged))
        self._updating_fields = False

    def sync_collection_buttons_from_text(self) -> None:
        if self._updating_fields:
            return

        selected = set(parse_csv_text(self.collections_edit.text())[:1])
        self._set_collection_buttons(selected)

        if selected:
            first = next(iter(selected))
            if self.collections_edit.text().strip() != first:
                self._updating_fields = True
                self.collections_edit.setText(first)
                self._updating_fields = False

    def sync_tag_buttons_from_text(self) -> None:
        if self._updating_fields:
            return

        selected = set(parse_csv_text(self.tags_edit.text()))
        self._set_tag_buttons(selected)

        normalized = normalize_csv_text(self.tags_edit.text())
        if self.tags_edit.text() != normalized:
            self._updating_fields = True
            self.tags_edit.setText(normalized)
            self._updating_fields = False

    def _set_collection_buttons(self, selected: set[str]) -> None:
        for name, button in self.collection_buttons.items():
            button.setChecked(name in selected)

    def _set_tag_buttons(self, selected: set[str]) -> None:
        for name, button in self.tag_buttons.items():
            button.setChecked(name in selected)

    def save_current_video(self) -> None:
        if self.current_video_id is None:
            QMessageBox.information(
                self,
                APP_NAME,
                "単体保存の対象がありません。複数選択中なら『選択中に一括適用』を使ってください。",
            )
            return

        self.db.update_video_metadata(
            video_id=self.current_video_id,
            title=self.title_edit.text().strip() or "Untitled",
            tags=normalize_csv_text(self.tags_edit.text()),
            collections=normalize_single_value(self.collections_edit.text()),
            note=self.note_edit.toPlainText().strip(),
        )
        self.reload_list()
        QMessageBox.information(self, APP_NAME, "保存しました。")

    def get_selected_video_ids(self) -> list[int]:
        return [int(item.data(Qt.UserRole)) for item in self.list_widget.selectedItems()]

    def apply_to_selected_videos(self) -> None:
        video_ids = self.get_selected_video_ids()
        if not video_ids:
            QMessageBox.information(self, APP_NAME, "先に動画を選択してください。")
            return

        collection_value = normalize_single_value(self.collections_edit.text())
        tag_value = normalize_csv_text(self.tags_edit.text())
        note_value = self.note_edit.toPlainText().strip()

        for video_id in video_ids:
            row = self.db.get_video(video_id)
            if row is None:
                continue

            self.db.update_video_metadata(
                video_id=video_id,
                title=row["title"] or "Untitled",
                tags=merge_csv_values(row["tags"] or "", tag_value),
                collections=collection_value if collection_value else (row["collections"] or ""),
                note=note_value if note_value else (row["note"] or ""),
            )

        self.reload_list()
        QMessageBox.information(self, APP_NAME, f"{len(video_ids)}件に一括適用しました。")

    def build_context_menu(self) -> QMenu:
        menu = QMenu(self)

        open_action = menu.addAction("既定アプリで開く")
        open_action.triggered.connect(self.open_selected_video)

        menu.addSeparator()

        collection_menu = menu.addMenu("コレクション設定")
        for name in PRESET_COLLECTIONS:
            action = collection_menu.addAction(name)
            action.triggered.connect(
                lambda checked=False, value=name: self.apply_quick_collection(value)
            )

        clear_collection_action = collection_menu.addAction("コレクション解除")
        clear_collection_action.triggered.connect(lambda: self.apply_quick_collection(""))

        tag_menu = menu.addMenu("タグ追加")
        for name in PRESET_TAGS:
            action = tag_menu.addAction(name)
            action.triggered.connect(
                lambda checked=False, value=name: self.apply_quick_tag(value)
            )

        menu.addSeparator()

        ignore_action = menu.addAction("選択中を無視")
        ignore_action.triggered.connect(self.ignore_selected_videos)

        return menu

    def show_list_context_menu(self, pos) -> None:
        item = self.list_widget.itemAt(pos)
        if item is None:
            return

        if not item.isSelected():
            self.list_widget.clearSelection()
            item.setSelected(True)

        menu = self.build_context_menu()
        menu.exec(self.list_widget.viewport().mapToGlobal(pos))

    def apply_quick_collection(self, collection_name: str) -> None:
        video_ids = self.get_selected_video_ids()
        if not video_ids:
            return

        for video_id in video_ids:
            row = self.db.get_video(video_id)
            if row is None:
                continue

            self.db.update_video_metadata(
                video_id=video_id,
                title=row["title"] or "Untitled",
                tags=row["tags"] or "",
                collections=collection_name,
                note=row["note"] or "",
            )

        self.reload_list()

    def apply_quick_tag(self, tag_name: str) -> None:
        video_ids = self.get_selected_video_ids()
        if not video_ids:
            return

        for video_id in video_ids:
            row = self.db.get_video(video_id)
            if row is None:
                continue

            self.db.update_video_metadata(
                video_id=video_id,
                title=row["title"] or "Untitled",
                tags=merge_csv_values(row["tags"] or "", tag_name),
                collections=row["collections"] or "",
                note=row["note"] or "",
            )

        self.reload_list()

    def ignore_selected_videos(self) -> None:
        video_ids = self.get_selected_video_ids()
        if not video_ids:
            QMessageBox.information(self, APP_NAME, "無視する動画を選択してください。")
            return

        reply = QMessageBox.question(
            self,
            APP_NAME,
            f"{len(video_ids)}件を無視しますか？\n元の動画ファイルは削除されません。\n再スキャン後も一覧に戻りません。",
            QMessageBox.Yes | QMessageBox.No,
            QMessageBox.No,
        )
        if reply != QMessageBox.Yes:
            return

        self.db.set_ignored(video_ids, ignored=True)
        self.reload_list()
        QMessageBox.information(self, APP_NAME, f"{len(video_ids)}件を無視しました。")

    def open_selected_video(self) -> None:
        selected_ids = self.get_selected_video_ids()
        if not selected_ids:
            return

        row = self.db.get_video(selected_ids[0])
        if row is None:
            return

        path = Path(row["absolute_path"])
        if not path.exists():
            QMessageBox.warning(self, APP_NAME, "ファイルが見つかりません。再スキャンしてください。")
            return

        try:
            if sys.platform.startswith("win"):
                os.startfile(str(path))
            elif sys.platform == "darwin":
                subprocess.Popen(["open", str(path)])
            else:
                subprocess.Popen(["xdg-open", str(path)])
        except Exception as e:
            QMessageBox.critical(self, APP_NAME, f"起動に失敗しました。\n{e}")

    def closeEvent(self, event) -> None:
        self.db.close()
        super().closeEvent(event)


def main() -> None:
    app = QApplication(sys.argv)
    window = MainWindow()
    window.show()
    sys.exit(app.exec())


if __name__ == "__main__":
    main()
