import csv
import os
from pathlib import Path
import random
# === RUTA CORRECTA BASADA EN ESTE ARCHIVO ===
# create_report.py está en app/scripts/, así que:
# parent      -> scripts
# parent.parent -> app
APP_DIR = Path(__file__).resolve().parent.parent

FTP_DIR = APP_DIR / "ftp_report_files"
FTP_DIR.mkdir(parents=True, exist_ok=True)  # crea la carpeta si no existe

archivo = FTP_DIR / "week_8_test_report.csv"
# =============================================
base_registros = [
    ["{sn}", "fto",       "fail", "0-A23"],
    ["{sn}", "swap",      "pass", ""     ],
    ["{sn}", "fto",       "fail", "0-A23"],
    ["{sn}", "swap",      "pass", ""     ],
    ["{sn}", "fto",       "fail", "0-A23"],
    ["{sn}", "swap",      "pass", ""     ],
    ["{sn}", "fto",       "pass", ""     ],
    ["{sn}", "runnin",    "pass", ""     ],
    ["{sn}", "test4",     "pass", ""     ],
    ["{sn}", "disk test", "pass", ""     ],
]

badges_list = [
        "222",  # fto
        "223",  # swap
        "333",  # fto
        "555",  # swap
        "666",  # fto
        "777",  # swap
        "888",  # fto
        "111",  # runnin
        "444",  # test4
        "999",  # disk test
    ]
set_badges =(list(set(badges_list)))
#print(list(set(set_badges)))
badges_por_serial = {
    "mxq": [
        random.choice((set_badges)),  # fto
        random.choice((set_badges)),  # swap
        random.choice((set_badges)),  # fto
        random.choice((set_badges)),  # swap
        random.choice((set_badges)),  # fto
        random.choice((set_badges)),  # swap
        random.choice((set_badges)),  # fto
        random.choice((set_badges)),  # runnin
        random.choice((set_badges)),  # test4
        random.choice((set_badges)),  # disk test
    ]
}

columnas = ["sn", "station", "status", "error_code", "badge"]
registros = []

# Aplicar a mxq1, mxq2, ..., mxq30000
for i in range(1, 30001):
    sn = f"mxq{i}"
    badges = badges_por_serial["mxq"]

    for idx, row in enumerate(base_registros):
        nueva = row.copy()
        nueva[0] = sn
        nueva.append(badges[idx])
        registros.append(nueva)

# Crear CSV
with open(archivo, "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow(columnas)
    writer.writerows(registros)

print("CSV generado:", archivo)