import sys
import json
from PyQt5.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
    QSplitter, QTextEdit, QPlainTextEdit, QLabel, QPushButton,
    QGroupBox, QTreeWidget, QTreeWidgetItem, QStatusBar,
    QMessageBox, QTabWidget, QSizePolicy, QSpinBox, QDialog,
    QDialogButtonBox, QFormLayout, QLineEdit
)
from PyQt5.QtCore import Qt, QThread, pyqtSignal, QObject
from PyQt5.QtGui import QFont, QColor

import preprocessing as pp
import annotation as ann
from annotation import AnnotationEngine, NODE_TYPE_NAMES



class AnnotationWorker(QObject):

    finished = pyqtSignal(list, dict, dict)
    error    = pyqtSignal(str)

    def __init__(self, conn_params: dict, query: str):
        super().__init__()
        self.conn_params = conn_params
        self.query = query

    def run(self):
        try:
            conn = pp.get_connection(**self.conn_params)

            qep = pp.get_qep(conn, self.query) #pass query to get_qep in preprocessing.py
            aqps = pp.get_aqps(conn, self.query) #pass query to get_aqps in preprocesing.py

            tables_map = ann.get_tables_from_sql(self.query)
            distict_tables = list(set(tables_map.values()))
            table_stats = pp.get_all_table_stats(conn, distict_tables)

            conn.close()

            engine = AnnotationEngine(qep, aqps, table_stats, self.query)
            annotations = engine.annotate()

            aqps_summary = {}
            for entry in aqps:
                label = entry.get("label", "")
                plan  = entry.get("plan")
                cost  = pp.get_plan_cost(plan) if plan else None
                aqps_summary[label] = cost

            self.finished.emit(annotations, qep, aqps_summary)

        except Exception as e:
            self.error.emit(str(e))


def build_tree(node: dict) -> QTreeWidgetItem:
    node_type = node.get("Node Type", "Unknown")
    cost = node.get("Total Cost", 0.0)
    rows = node.get("Plan Rows", "?")
    relation = node.get("Relation Name", "")

    label = f"{node_type}"
    if relation:
        label += f"[{relation}]"
    label += f"cost={cost:.2f}  Est Row={rows}"

    item = QTreeWidgetItem([label])
    item.setData(0, Qt.UserRole, node)

    if "Join" in node_type or node_type == "Nested Loop":
        item.setForeground(0, QColor("#0057AE"))
    elif "Scan" in node_type:
        item.setForeground(0, QColor("#006400"))
    elif node_type in ("Sort", "Incremental Sort"):
        item.setForeground(0, QColor("#8B4513"))
    elif "Aggregate" in node_type:
        item.setForeground(0, QColor("#800080"))

    for child in node.get("Plans", []):
        item.addChild(build_tree(child))

    return item


class ConnectionDialog(QDialog):
    def __init__(self, defaults: dict, parent=None):
        super().__init__(parent)
        self.setWindowTitle("PostgreSQL Connection Settings")
        self.setMinimumWidth(360)

        layout = QFormLayout(self)

        self.host_edit = QLineEdit(defaults.get("host", "localhost"))
        self.port_spin = QSpinBox()
        self.port_spin.setRange(1, 65535)
        self.port_spin.setValue(defaults.get("port", 5432))
        self.db_edit   = QLineEdit(defaults.get("dbname", "TPC-H"))
        self.user_edit = QLineEdit(defaults.get("user", "postgres"))
        self.pw_edit   = QLineEdit(defaults.get("password", "sudo"))
        self.pw_edit.setEchoMode(QLineEdit.Password)

        layout.addRow("Host:", self.host_edit)
        layout.addRow("Port:", self.port_spin)
        layout.addRow("Database:", self.db_edit)
        layout.addRow("User:", self.user_edit)
        layout.addRow("Password:", self.pw_edit)

        buttons = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        buttons.accepted.connect(self.accept)
        buttons.rejected.connect(self.reject)
        layout.addRow(buttons)

    def get_params(self) -> dict:
        return {
            "host": self.host_edit.text().strip(),
            "port": self.port_spin.value(),
            "dbname": self.db_edit.text().strip(),
            "user": self.user_edit.text().strip(),
            "password": self.pw_edit.text(),
        }


class MainWindow(QMainWindow):

    DEFAULT_CON = {
        "host":     "localhost",
        "port":     5432,
        "dbname":   "TPC-H",
        "user":     "postgres",
        "password": "sudo",
    }

    def __init__(self):
        super().__init__()
        self.setWindowTitle("SC3020 — SQL Query Plan Annotator")
        self.resize(1280, 800)

        self._connections = dict(self.DEFAULT_CON)
        self._worker_thread  = None
        self._worker = None
        self._annotations = []
        self._qep = {}

        self.set_ui()
        self.set_stylesheet()

    def set_ui(self):
        central = QWidget()
        self.setCentralWidget(central)
        root_layout = QVBoxLayout(central)
        root_layout.setContentsMargins(8, 8, 8, 8)
        root_layout.setSpacing(6)

        root_layout.addWidget(self.set_toolbar())

        main_splitter = QSplitter(Qt.Vertical)
        root_layout.addWidget(main_splitter)

        top_splitter = QSplitter(Qt.Horizontal)
        top_splitter.addWidget(self.set_editor_panel())
        top_splitter.addWidget(self.set_qep_panel())
        top_splitter.setSizes([600, 500])
        main_splitter.addWidget(top_splitter)

        main_splitter.addWidget(self.set_annotation_panel())
        main_splitter.setSizes([420, 320])

        self.status_bar = QStatusBar()
        self.setStatusBar(self.status_bar)
        self.status_bar.showMessage("Ready — click Connect to connect to PostgreSQL, then paste a query and click Annotate.")

    def set_toolbar(self) -> QWidget:
        bar = QGroupBox()
        bar.setFlat(True)
        layout = QHBoxLayout(bar)
        layout.setContentsMargins(0, 0, 0, 0)

        self.conn_btn = QPushButton("DB Settings") #set button name
        self.conn_btn.setToolTip("Configure PostgreSQL connection") #hover to see tips
        self.conn_btn.clicked.connect(self.open_connection_dialog) #when button is clicked call open_connection_dialog

        self.connect_btn = QPushButton("Connect")
        self.connect_btn.setFixedWidth(100)
        self.connect_btn.clicked.connect(self.test_connection) #when connect button is click, test connection method is executed

        self.conn_status_label = QLabel("● Not connected")
        self.conn_status_label.setStyleSheet("color: #cc0000; font-weight: bold;")

        self.run_btn = QPushButton("▶  Annotate Query")
        self.run_btn.setFixedHeight(34)
        self.run_btn.setEnabled(False)
        self.run_btn.clicked.connect(self.run_annotation) # when button is clicked, run annotation method is called

        self.clear_btn = QPushButton("Clear")
        self.clear_btn.setFixedWidth(80)
        self.clear_btn.clicked.connect(self.clear_all)

        layout.addWidget(self.conn_btn)
        layout.addWidget(self.connect_btn)
        layout.addWidget(self.conn_status_label)
        layout.addStretch()
        layout.addWidget(self.run_btn)
        layout.addWidget(self.clear_btn)
        return bar

    def set_editor_panel(self) -> QGroupBox:
        box    = QGroupBox("SQL Query Editor")
        layout = QVBoxLayout(box)

        self.sql_editor = QPlainTextEdit()
        self.sql_editor.setFont(QFont("Courier New", 11))
        self.sql_editor.setPlaceholderText(
            "Paste or type your SQL query here\n"
        )

        layout.addWidget(self.sql_editor)
        return box

    def set_qep_panel(self) -> QGroupBox:
        box = QGroupBox("Query Execution Plan (QEP Tree)")
        layout = QVBoxLayout(box)

        self.qep_tree = QTreeWidget()
        self.qep_tree.setHeaderLabel("Plan Node   [Relation]   cost   rows")
        self.qep_tree.setFont(QFont("Courier New", 10))
        self.qep_tree.itemClicked.connect(self.on_qep_node_clicked)
        layout.addWidget(self.qep_tree)

        layout.addWidget(QLabel("Node Details:"))
        self.node_detail = QTextEdit()
        self.node_detail.setReadOnly(True)
        self.node_detail.setMaximumHeight(130)
        self.node_detail.setFont(QFont("Courier New", 9))
        self.node_detail.setPlaceholderText(
            "Click a node above to see its full details.")
        layout.addWidget(self.node_detail)
        return box

    def set_annotation_panel(self) -> QTabWidget:
        tabs = QTabWidget()

        self.annotated_view = QTextEdit()
        self.annotated_view.setReadOnly(True)
        self.annotated_view.setFont(QFont("Courier New", 11))
        tabs.addTab(self.annotated_view, "Annotated Query")

        self.annotation_list = QTextEdit()
        self.annotation_list.setReadOnly(True)
        self.annotation_list.setFont(QFont("Arial", 10))
        tabs.addTab(self.annotation_list, "Annotation Details")

        self.aqp_view = QTextEdit()
        self.aqp_view.setReadOnly(True)
        self.aqp_view.setFont(QFont("Courier New", 10))
        tabs.addTab(self.aqp_view, "AQP Cost Comparison")

        self.raw_qep_view = QPlainTextEdit()
        self.raw_qep_view.setReadOnly(True)
        self.raw_qep_view.setFont(QFont("Courier New", 9))
        tabs.addTab(self.raw_qep_view, "Raw QEP (JSON)")

        return tabs

    def set_stylesheet(self):
        self.setStyleSheet("""
            QMainWindow { background: #f5f5f5; }
            QGroupBox {
                font-weight: bold;
                border: 1px solid #cccccc;
                border-radius: 4px;
                margin-top: 8px;
                padding: 6px;
            }
            QGroupBox::title {
                subcontrol-origin: margin;
                left: 8px;
                padding: 0 4px;
            }
            QPushButton {
                background: #0057AE;
                color: white;
                border: none;
                border-radius: 4px;
                padding: 6px 12px;
                font-weight: bold;
            }
            QPushButton:hover   { background: #0070d8; }
            QPushButton:pressed { background: #003d7a; }
            QPushButton:disabled { background: #aaaaaa; }
            QPlainTextEdit, QTextEdit {
                background: #ffffff;
                border: 1px solid #cccccc;
                border-radius: 3px;
            }
            QTreeWidget {
                background: #ffffff;
                border: 1px solid #cccccc;
                border-radius: 3px;
            }
            QTabWidget::pane { border: 1px solid #cccccc; }
            QTabBar::tab {
                padding: 6px 14px;
                background: #e0e0e0;
                border: 1px solid #cccccc;
                border-bottom: none;
                border-radius: 4px 4px 0 0;
            }
            QTabBar::tab:selected { background: #ffffff; font-weight: bold; }
        """)

    def open_connection_dialog(self):
        dlg = ConnectionDialog(self._connections, self)
        if dlg.exec_() == QDialog.Accepted: # when dialog window is open wait for user to make decision
            self._connections = dlg.get_params() # get value from user and save
            self.status_bar.showMessage(
                f"Settings updated — click Connect to test.")

    def test_connection(self):
        self.status_bar.showMessage("Testing connection…")
        QApplication.processEvents() # this is the refresh the app
        test_ok = pp.test_connection(**self._connections) #unpack dictionary and pass into test connection method in preprocessing.py
        if test_ok:
            self.conn_status_label.setText("● Connected")
            self.conn_status_label.setStyleSheet("color: #006400; font-weight: bold;")
            self.run_btn.setEnabled(True)
            self.status_bar.showMessage(
                f"Connected to {self._connections['dbname']} on "
                f"{self._connections['host']}:{self._connections['port']}")
        else:
            self.conn_status_label.setText("● Not connected")
            self.conn_status_label.setStyleSheet("color: #cc0000; font-weight: bold;")
            self.run_btn.setEnabled(False)
            QMessageBox.critical(self, "Connection Failed", "Could not connect to PostgreSQL.\n" "Please check your settings and try again.")

    def run_annotation(self):
        query = self.sql_editor.toPlainText().strip()
        if not query:
            QMessageBox.warning(self, "No Query", "Please enter an SQL query first.")
            return

        self.run_btn.setEnabled(False)
        self.status_bar.showMessage(
            "Retrieving QEP and AQPs from PostgreSQL…")
        self.clear_outputs()

        self._worker_thread = QThread() #create thread
        self._worker = AnnotationWorker(self._connections, query)
        self._worker.moveToThread(self._worker_thread)
        self._worker_thread.started.connect(self._worker.run)
        self._worker.finished.connect(self.on_annotation_done)
        self._worker.error.connect(self.on_annotation_error)
        self._worker.finished.connect(self._worker_thread.quit)
        self._worker.error.connect(self._worker_thread.quit)
        self._worker_thread.start()

    def clear_all(self):
        self.sql_editor.clear()
        self.clear_outputs()

    def clear_outputs(self):
        self.qep_tree.clear()
        self.node_detail.clear()
        self.annotated_view.clear()
        self.annotation_list.clear()
        self.aqp_view.clear()
        self.raw_qep_view.clear()

    def on_annotation_done(self, annotations: list, qep: dict, aqps_summary: dict):
        self._annotations = annotations
        self._qep = qep
        query = self.sql_editor.toPlainText().strip()

        self.generate_qep_tree(qep)
        self.generate_annotated_view(query, annotations)
        self.generate_annotation_list(annotations)
        self.generate_aqp_view(aqps_summary, annotations)
        self.raw_qep_view.setPlainText(json.dumps(qep, indent=2))

        self.run_btn.setEnabled(True)
        self.status_bar.showMessage(
            f"Done — {len(annotations)} annotation(s) generated.")

    def on_annotation_error(self, msg: str):
        self.run_btn.setEnabled(True)
        self.status_bar.showMessage("Error during annotation.")
        QMessageBox.critical(self, "Annotation Error", msg)

    def generate_qep_tree(self, qep: dict):
        self.qep_tree.clear()
        plan_node = qep.get("Plan", qep)
        root_item = build_tree(plan_node)
        self.qep_tree.addTopLevelItem(root_item)
        self.qep_tree.expandAll()

    def on_qep_node_clicked(self, item: QTreeWidgetItem, _col: int):
        node = item.data(0, Qt.UserRole)
        if node:
            interesting_keys = [
                "Node Type", "Relation Name", "Index Name", "Index Cond",
                "Filter", "Hash Cond", "Merge Cond", "Join Filter",
                "Sort Key", "Group Key", "Strategy",
                "Startup Cost", "Total Cost", "Plan Rows", "Plan Width",
            ]
            lines = []
            for k in interesting_keys:
                if k in node:
                    lines.append(f"{k:20s}: {node[k]}")
            self.node_detail.setPlainText("\n".join(lines))

    def generate_annotated_view(self, query: str, annotations: list):
        import html as html_lib

        COLOURS = ["#0057AE", "#006400", "#8B0000",
                   "#800080", "#8B4513", "#005555"]

        frag_colour = {}
        for i, a in enumerate(annotations):
            if a.sql_cat == "JOIN":
                continue
            frag = a.sql_fragment.strip()
            if frag:
                frag_colour[frag] = COLOURS[i % len(COLOURS)]

        escaped = html_lib.escape(query)
        for frag, colour in frag_colour.items():
            esc_frag = html_lib.escape(frag)
            escaped  = escaped.replace(
                esc_frag,
                f'<span style="background:{colour}22; '
                f'border-bottom:2px solid {colour}; '
                f'color:{colour}; font-weight:bold;">'
                f'{esc_frag}</span>',
                1
            )

        html_lines = [
            "<pre style='font-family:Courier New; font-size:11pt;'>",
            escaped,
            "</pre><hr/>",
            "<table style='font-family:Arial; font-size:10pt; width:100%;'>",
        ]
        for i, a in enumerate(annotations):
            colour = COLOURS[i % len(COLOURS)]
            html_lines.append(
                f"<tr>"
                f"<td style='width:12px; background:{colour};'>&nbsp;</td>"
                f"<td style='padding:4px 8px;'>"
                f"<b>[{a.sql_cat}] {a.operator}</b><br/>"
                f"<i>{html_lib.escape(a.sql_fragment)}</i><br/>"
                f"{html_lib.escape(a.reason)}"
                f"</td></tr>"
            )
        html_lines.append("</table>")
        self.annotated_view.setHtml("".join(html_lines))

    def generate_annotation_list(self, annotations: list):
        import html as html_lib
        lines = ["<html><body style='font-family:Arial; font-size:10pt;'>",
                 "<h3>Annotation Details</h3>"]
        for i, a in enumerate(annotations, 1):
            lines.append(f"<h4>#{i} [{a.sql_cat}] — {a.operator}</h4>")
            lines.append(
                f"<b>SQL fragment:</b> "
                f"<code>{html_lib.escape(a.sql_fragment)}</code><br/>")
            lines.append(f"<b>Reason:</b> {html_lib.escape(a.reason)}<br/>")
            lines.append(f"<b>QEP cost:</b> {a.cost_qep:.4f}<br/>")
            if a.cost_alts:
                alts = ", ".join(
                    f"{NODE_TYPE_NAMES.get(k, k)}: {v:.2f}"
                    for k, v in a.cost_alts.items()
                )
                lines.append(
                    f"<b>Alternative costs:</b> {html_lib.escape(alts)}<br/>")
            lines.append("<hr/>")
        lines.append("</body></html>")
        self.annotation_list.setHtml("".join(lines))

    def generate_aqp_view(self, aqps_summary: dict, annotations: list):
        import html as html_lib
        qep_cost_str = ""
        for a in annotations:
            if a.cost_qep > 0:
                qep_cost_str = f"{a.cost_qep:.2f}"
                break

        lines = [
            "<html><body style='font-family:Arial; font-size:10pt;'>",
            "<h3>Alternative Query Plans</h3>",
            "<table border='1' cellpadding='4' cellspacing='0' "
            "style='border-collapse:collapse; width:100%;'>",
            "<tr style='background:#e0e0e0;'>"
            "<th>Configuration</th><th>Estimated Cost</th></tr>",
            f"<tr style='background:#c8e6c9;'>"
            f"<td><b>QEP Plans</b></td>"
            f"<td><b>{qep_cost_str}</b></td></tr>",
        ]
        for label, cost in aqps_summary.items():
            cost_str = (f"{cost:.2f}" if cost is not None
                        else "N/A (plan not possible)")
            lines.append(
                f"<tr><td>{html_lib.escape(label)}</td>"
                f"<td>{cost_str}</td></tr>")
        lines.append("</table></body></html>")
        self.aqp_view.setHtml("".join(lines))

#create Qapplication and create main window
def open_gui():
    app = QApplication(sys.argv)
    app.setApplicationName("SC3020 QEP Annotator")
    window = MainWindow()
    window.show()
    sys.exit(app.exec_())
