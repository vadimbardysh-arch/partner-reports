"""Provider configuration for report generation."""

SERVER_HOSTNAME = "bolt-incentives.cloud.databricks.com"
HTTP_PATH = "sql/protocolv1/o/2472566184436351/0221-081903-9ag4bh69"

PROVIDERS = {
    31504: {"name": "Epic Cheeseburger", "slug": "epic-cheeseburger", "city": "Львів"},
    187635: {"name": "EPIC CHEESEBURGER CAFE DL", "slug": "epic-cheeseburger-cafe-dl", "city": "Львів"},
    31506: {"name": "TEDDY", "slug": "teddy", "city": "Львів"},
    31505: {"name": "МОРЕ РИБИ", "slug": "more-ryby", "city": "Львів"},
    31502: {"name": "Cukor Red", "slug": "cukor-red", "city": "Львів"},
    31503: {"name": "Cukor Black", "slug": "cukor-black", "city": "Львів"},
}

WEEKS_BACK = 8
