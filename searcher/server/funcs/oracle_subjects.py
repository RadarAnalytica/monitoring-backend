from collections import Counter


def filter_subjects_list(subjects_list: list[int]) -> list[int] | None:
    """
    Единая логика отбора предметов для ниш (monitoring_oracle).
    Оставляет предметы с долей >= 10% в топ-300, либо единственный предмет.
    Возвращает None, если список пуст после фильтрации.
    """
    if not subjects_list:
        return None

    subjects_set = set(subjects_list)
    if len(subjects_set) == 1:
        return list(subjects_set)

    total_subjects = len(subjects_list)
    counted_subjects = Counter(subjects_list)
    filtered_subjects = [
        subject_id
        for subject_id in subjects_set
        if counted_subjects[subject_id] / total_subjects >= 0.10
    ]
    filtered_subjects.sort()
    return filtered_subjects or None
