# app/tasks.py
import logging
from pathlib import Path
from celery.utils.log import get_task_logger
from .celery_app import celery_app
import os
import glob
import re

_num_re = re.compile(r'(week|month)_(\d+)', re.IGNORECASE)

def _period_key(path_str: str) -> tuple[int, str]:
    """
    Extrae el número de periodo del nombre. Ej: week_8_test_report.csv -> (8, 'week')
    Si no lo encuentra, devuelve (0, 'zzz') para que quede al final.
    """
    name = Path(path_str).name
    m = _num_re.search(name)
    if m:
        kind = m.group(1).lower()  # 'week' o 'month'
        num = int(m.group(2))
        return (num, kind)
    return (0, "zzz")

from .perf_logic import run_performance_from_csv, write_txt_report

logger = get_task_logger(__name__)
logging.basicConfig(level=logging.INFO)

# Directorio donde llegan los archivos
DEFAULT_DIR = os.getenv("FTP_REPORT_DIR", str(Path(__file__).resolve().parent / "ftp_report_files"))
# Directorio donde guardaremos los reportes .txt
REPORTS_DIR = os.getenv("REPORTS_DIR", str(Path(__file__).resolve().parent / "reports"))

@celery_app.task(name="app.tasks.check_ftp_reports")
def check_ftp_reports(directory: str | None = None) -> str:
    """Solo informa si existen archivos."""
    dir_path = Path(directory or DEFAULT_DIR)
    week_matches  = glob.glob(str(dir_path / "week_*_test_report.csv"))
    month_matches = glob.glob(str(dir_path / "month_*_test_report.csv"))

    count = int(len(week_matches) > 0) + int(len(month_matches) > 0)
    if count == 0:
        msg = "no encontre nada"
    elif count == 1:
        msg = "encontre 1 archivo"
    else:
        msg = "encontre 2 archivos"

    logger.info("[check_ftp_reports] Dir=%s | week=%s | month=%s | %s",
                dir_path, bool(week_matches), bool(month_matches), msg)
    return msg

@celery_app.task(name="app.tasks.run_performance_if_reports")
def run_performance_if_reports(directory: str | None = None) -> dict:
    dir_path = Path(directory or DEFAULT_DIR)
    reports_dir = Path(REPORTS_DIR)
    reports_dir.mkdir(parents=True, exist_ok=True)

    # Encontrar TODOS los archivos
    week_matches  = list(dir_path.glob("week_*_test_report.csv"))
    month_matches = list(dir_path.glob("month_*_test_report.csv"))

    # Ordenar por número de periodo (semana o mes)
    week_matches  = sorted((str(p) for p in week_matches),  key=_period_key)   # week_7,..., week_8,...
    month_matches = sorted((str(p) for p in month_matches), key=_period_key)

    out = {"week": [], "month": []}
    processed_paths = []

    # --- Primero SEMANA: procesar todos ---
    for week_csv in week_matches:
        try:
            df_res, meta = run_performance_from_csv(week_csv)
            week_txt = reports_dir / (Path(week_csv).stem + "_report.txt")
            write_txt_report(str(week_txt), df_res, meta, title=f"Reporte Semanal ({Path(week_csv).name})")
            logger.info("[run_performance_if_reports] Semanal OK | in=%s | out=%s", week_csv, week_txt)
            out["week"].append(str(week_txt))
            processed_paths.append(week_csv)
        except Exception as e:
            logger.exception("[run_performance_if_reports] Error procesando semanal %s: %s", week_csv, e)

    # --- Luego MES: procesar todos ---
    for month_csv in month_matches:
        try:
            df_res, meta = run_performance_from_csv(month_csv)
            month_txt = reports_dir / (Path(month_csv).stem + "_report.txt")
            write_txt_report(str(month_txt), df_res, meta, title=f"Reporte Mensual ({Path(month_csv).name})")
            logger.info("[run_performance_if_reports] Mensual OK | in=%s | out=%s", month_csv, month_txt)
            out["month"].append(str(month_txt))
            processed_paths.append(month_csv)
        except Exception as e:
            logger.exception("[run_performance_if_reports] Error procesando mensual %s: %s", month_csv, e)

    # Borrar SOLO lo que se procesó con éxito
    for p in processed_paths:
        try:
            Path(p).unlink()
        except Exception as e:
            logger.warning("[run_performance_if_reports] No pude borrar %s: %s", p, e)

    if not processed_paths:
        logger.info("[run_performance_if_reports] No hay archivos de entrada para procesar.")

    return out


def cleanup_report_inputs(directory: str | Path, patterns: tuple[str, ...] = (
    "week_*_test_report.csv",
    "month_*_test_report.csv",)):
    """Elimina los archivos de entrada para evitar reprocesos cada minuto."""
    dir_path = Path(directory)
    removed = []
    for pat in patterns:
        for p in dir_path.glob(pat):
            try:
                p.unlink()
                removed.append(str(p))
            except Exception as e:
                logger.warning("No pude borrar %s: %s", p, e)
    if removed:
        logger.info("[cleanup] Eliminados: %s", removed)
    else:
        logger.info("[cleanup] No había archivos para eliminar.")

