import requests
from typing import List, Dict

BASE_URL = "https://api.hh.ru/"


def get_vacancies_for_company(employer_id: str = None, pages: int = 1) -> List[Dict]:
    """
    Получение вакансий с API hh.ru. Если не передан employer_id, то вакансии берутся для всех компаний.
    :param employer_id: ID компании. Если None, возвращаются вакансии для всех компаний.
    :param pages: Количество страниц для загрузки (по умолчанию 1).
    :return: Список вакансий.
    """
    all_vacancies = []
    params = {'per_page': 100}  # Без ID компании, все вакансии из всех компаний
    url = f"{BASE_URL}vacancies"

    if employer_id:
        params['employer_id'] = employer_id  # Только вакансии этой компании

    for page_number in range(pages):
        params['page'] = page_number
        response = requests.get(url, params=params)

        if response.status_code == 200:
            data = response.json()
            if not data.get('items'):
                print(f"Нет вакансий на странице {page_number + 1}")
                break
            all_vacancies.extend(data.get('items', []))
            if len(data.get('items', [])) < 100:
                break  # Останавливаемся, если на текущей странице меньше 100 вакансий
        else:
            print(f"Ошибка при запросе: {response.status_code} - {response.text}")
            break

    return all_vacancies
