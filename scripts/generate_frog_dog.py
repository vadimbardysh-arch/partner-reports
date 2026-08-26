"""Generate the FROG & DOG weekly partner report from the latest dashboard template."""

import re
from pathlib import Path


TEMPLATE_PATH = Path(__file__).with_name("generate_miasorub.py")

FROG_DOG_PROVIDERS = """FROG_DOG_PROVIDERS = {
    173689: {"name": "FROG & DOG Невелика", "short": "Невелика", "city": "Lviv"},
    133292: {"name": "FROG & DOG вул. Городоцька", "short": "Городоцька", "city": "Lviv"},
    107979: {"name": "FROG & DOG вул. Навроцького", "short": "Навроцького", "city": "Lviv"},
    62874:  {"name": "FROG & DOG Хімічна", "short": "Хімічна", "city": "Lviv"},
    37405:  {"name": "FROG & DOG Гетьмана Мазепи", "short": "Гетьмана Мазепи", "city": "Lviv"},
    37434:  {"name": "FROG & DOG Широка 100", "short": "Широка", "city": "Lviv"},
    37404:  {"name": "FROG & DOG Зубрівська", "short": "Зубрівська", "city": "Lviv"},
}
"""


def load_generator():
    source = TEMPLATE_PATH.read_text(encoding="utf-8")
    providers_start = source.index("MIASORUB_PROVIDERS = {")
    providers_end = source.index("\n\nPROVIDER_IDS =", providers_start)
    source = source[:providers_start] + FROG_DOG_PROVIDERS.rstrip() + source[providers_end:]
    source = source.replace("MIASORUB_PROVIDERS", "FROG_DOG_PROVIDERS")
    source = source.replace("Мʼясоруб", "FROG & DOG")
    source = source.replace("МʼЯСОРУБ", "FROG & DOG")
    source = source.replace("miasorub", "frog-dog")
    source = re.sub(r"\n\.calc-card\{\{[^\n]+\}\}", "", source)
    return compile(source, str(TEMPLATE_PATH), "exec")


exec(load_generator(), globals())
