from datetime import date, timedelta


def get_month_start(value: date) -> date:
    return value.replace(day=1)


def get_next_month(value: date) -> date:

    if value.month == 12:
        return date(
            value.year + 1,
            1,
            1
        )

    return date(
        value.year,
        value.month + 1,
        1
    )


def get_previous_month_start(
    value: date
) -> date:

    first_day = get_month_start(value)

    if first_day.month == 1:
        return date(
            first_day.year - 1,
            12,
            1
        )

    return date(
        first_day.year,
        first_day.month - 1,
        1
    )


def create_download_plan(
    start_date: date,
    end_date: date
) -> list:

    plan = []

    today = date.today()

    # Never process today's incomplete daily data.
    safe_end_date = min(
        end_date,
        today - timedelta(days=1)
    )

    if start_date > safe_end_date:
        return plan

    current = start_date

    # --------------------------------------------------
    # 1. Partial starting month → DAILY
    # --------------------------------------------------

    if current.day != 1:

        month_end = (
            get_next_month(current)
            - timedelta(days=1)
        )

        daily_end = min(
            month_end,
            safe_end_date
        )

        while current <= daily_end:

            plan.append({
                "frequency": "daily",
                "date": current
            })

            current += timedelta(days=1)

        current = get_next_month(
            get_month_start(current)
        )

    # --------------------------------------------------
    # 2. Complete months → MONTHLY
    # --------------------------------------------------

    while True:

        next_month = get_next_month(current)

        month_end = (
            next_month
            - timedelta(days=1)
        )

        if month_end > safe_end_date:
            break

        plan.append({
            "frequency": "monthly",
            "year": current.year,
            "month": current.month
        })

        current = next_month

    # --------------------------------------------------
    # 3. Remaining partial month → DAILY
    # --------------------------------------------------

    while current <= safe_end_date:

        plan.append({
            "frequency": "daily",
            "date": current
        })

        current += timedelta(days=1)

    return plan