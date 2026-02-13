from __future__ import annotations

from decimal import Decimal, InvalidOperation


CATEGORY_TITLES = {
    "sugar": "Шугаринг",
    "laser": "Лазер",
}


_SERVICE_NAME_PREFIXES = (
    "Шугаринг:",
    "Лазерная эпиляция:",
    "Лазер:",
)


def format_price(value: object) -> str:
    if value is None:
        return ""
    try:
        normalized = f"{Decimal(str(value)):.2f}"
    except (InvalidOperation, ValueError):
        return str(value)
    return normalized.rstrip("0").rstrip(".")


def appointment_services_label(appt) -> str:
    comment = (getattr(appt, "admin_comment", None) or "").strip()
    if comment.lower().startswith("услуги:"):
        label = comment.split(":", 1)[1].strip()
        if label:
            return label
    service = getattr(appt, "service", None)
    if service and getattr(service, "name", None):
        return service_label_with_category(service)
    return "Услуга"


def service_category_title(category: str | None) -> str:
    if not category:
        return "Услуги"
    return CATEGORY_TITLES.get(category, category)


def service_label_with_category(service) -> str:
    name = (getattr(service, "name", None) or "").strip()
    for prefix in _SERVICE_NAME_PREFIXES:
        if name.startswith(prefix):
            name = name[len(prefix):].strip()
            break
    category = service_category_title(getattr(service, "category", None))
    if not name:
        return category
    return f"{category}: {name}"


def services_label_with_category(services: list) -> str:
    return ", ".join(service_label_with_category(s) for s in services)
