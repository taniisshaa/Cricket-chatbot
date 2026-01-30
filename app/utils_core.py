import logging
import os
import json
import tempfile
from datetime import datetime
from dotenv import load_dotenv
load_dotenv(override=True)
def get_logger(name="app", log_file=None):
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    # Check if handlers are already added to avoid duplicates
    if not logger.handlers:
        # Create formatter
        formatter = logging.Formatter('%(asctime)s | %(levelname)s | %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

        # Console Handler
        c_handler = logging.StreamHandler()
        c_handler.setFormatter(formatter)
        logger.addHandler(c_handler)

        # File Handler (if log_file provided)
        if log_file:
            log_dir = "logs"
            if not os.path.exists(log_dir):
                os.makedirs(log_dir)
            
            file_path = os.path.join(log_dir, log_file)
            f_handler = logging.FileHandler(file_path, encoding='utf-8')
            f_handler.setFormatter(formatter)
            logger.addHandler(f_handler)

    logger.propagate = False
    return logger
logger = get_logger("app_main")
class Config:
    DB_PATH = os.path.join("data", "cricket_data_v2.db")
    OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
    @staticmethod
    def ensure_dirs():
        dirs = ["data", "fonts"]
        for d in dirs:
            if not os.path.exists(d):
                os.makedirs(d)
def load_chat():
    path = os.path.join("data", "chat_history.json")
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"Failed to load chat: {e}")
            return []
    return []
def save_chat(messages):
    path = os.path.join("data", "chat_history.json")
    Config.ensure_dirs()
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(messages, f, ensure_ascii=False, indent=2)
    except Exception as e:
        logger.error(f"Failed to save chat: {e}")
def generate_chat_pdf(messages):
    import os
    from reportlab.lib.pagesizes import A4
    from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer
    from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
    from reportlab.pdfbase import pdfmetrics
    from reportlab.pdfbase.ttfonts import TTFont
    from xml.sax.saxutils import escape
    pdf_path = None
    try:
        tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".pdf")
        pdf_path = tmp.name
        tmp.close()
        nirmala_path = "C:/Windows/Fonts/Nirmala.ttf"
        noto_path = os.path.join(os.getcwd(), "fonts", "NotoSansDevanagari-Regular.ttf")
        font_name = "SystemFont"
        target_font = None
        if os.path.exists(nirmala_path):
            target_font = nirmala_path
            logger.info("Using System Font: Nirmala UI")
        elif os.path.exists(noto_path):
            target_font = noto_path
            logger.info("Using Project Font: Noto Sans Devanagari")
        if target_font:
            try:
                pdfmetrics.registerFont(TTFont(font_name, target_font))
            except Exception as fe:
                logger.error(f"Font registration failed: {fe}")
                font_name = "Helvetica"
        else:
            logger.warning("No Hindi-capable font found. Using Helvetica (Hindi will break).")
            font_name = "Helvetica"
        doc = SimpleDocTemplate(
            pdf_path,
            pagesize=A4,
            rightMargin=40,
            leftMargin=40,
            topMargin=40,
            bottomMargin=40
        )
        styles = getSampleStyleSheet()
        title_style = ParagraphStyle(
            "TitleStyle",
            parent=styles["Title"],
            fontName=font_name,
            fontSize=18,
            spaceAfter=25,
            alignment=1
        )
        q_style = ParagraphStyle(
            "QuestionStyle",
            parent=styles["Normal"],
            fontName=font_name,
            fontSize=11,
            leading=16,
            spaceBefore=12,
            spaceAfter=4,
            textColor="#202124"
        )
        a_style = ParagraphStyle(
            "AnswerStyle",
            parent=styles["Normal"],
            fontName=font_name,
            fontSize=11,
            leading=16,
            leftIndent=20,
            spaceAfter=12,
            textColor="#1a73e8"
        )
        story = []
        story.append(Paragraph("<b>Cricket Chatbot – Conversation History 🏏</b>", title_style))
        story.append(Spacer(1, 10))
        for msg in messages:
            content = escape(msg.get("content", "")).replace("\n", "<br/>")
            if msg.get("role") == "user":
                story.append(Paragraph(f"<b>Q.</b> {content}", q_style))
            else:
                story.append(Paragraph(f"<b>A.</b> {content}", a_style))
        doc.build(story)
        with open(pdf_path, "rb") as f:
            pdf_bytes = f.read()
        if pdf_path and os.path.exists(pdf_path):
            os.remove(pdf_path)
        return pdf_bytes
    except Exception as e:
        logger.error(f"PDF generation error: {e}")
        if pdf_path and os.path.exists(pdf_path):
            try: os.remove(pdf_path)
            except: pass
        return b""
