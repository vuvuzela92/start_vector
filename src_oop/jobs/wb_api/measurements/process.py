from typing import List, Dict, Any


def process_measurements_data(data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    process_data = []
    for f in data:
        data_dict = {
            "nm_id": f.get("nmId"),
            "subjectName": f.get("subjectName"),
            "dim_id": f.get("dimId"),
            "volume": f.get("volume"),
            "width": f.get("width"),
            "length": f.get("length"),
            "height": f.get("height"),
            "photo_urls": f.get("photoUrls")[0] if f.get("photoUrls") else None,
            "dt": f.get("dt"),
            "account": f.get("account")  # Добавляем информацию об аккаунте в каждый замер
        }
        data_dict["date"] = data_dict["dt"][:10]  # Извлекаем только дату в формате YYYY-MM-DD  
        process_data.append(data_dict)
    return process_data