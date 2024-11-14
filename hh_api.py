import requests
from concurrent.futures import ThreadPoolExecutor
import time

BASE_URL = "https://api.hh.ru/"


def get_vacancies_for_company(company_id=None, pages=1):
    all_vacancies = []
    params = {'employer_id': company_id, 'per_page': 100} if company_id else {'per_page': 100}
    url = f"{BASE_URL}vacancies"

    for page in range(pages):
        params['page'] = page
        response = requests.get(url, params=params)
        if response.status_code == 200:
            data = response.json()
            all_vacancies.extend(data.get('items', []))
            if len(data.get('items', [])) < 100:
                break
        else:
            print(f"Ошибка при получении данных о вакансиях: {response.status_code}")
            break

    return all_vacancies


def get_all_vacancies_for_companies(companies=None):
    all_vacancies = {}
    start_time = time.time()
    with ThreadPoolExecutor(max_workers=10) as executor:
        if companies:
            futures = {executor.submit(get_vacancies_for_company, company['id'], 5): company['name'] for company in
                       companies}
            for future in futures:
                company_name = futures[future]
                try:
                    vacancies = future.result()
                    if vacancies:
                        all_vacancies[company_name] = vacancies
                except Exception as e:
                    print(f"Ошибка при получении данных для компании {company_name}: {e}")
        else:
            all_vacancies_list = get_vacancies_for_company(pages=5)
            for vacancy in all_vacancies_list:
                company_name = vacancy['employer']['name']
                if company_name not in all_vacancies:
                    all_vacancies[company_name] = []
                all_vacancies[company_name].append(vacancy)

    execution_time = round(time.time() - start_time, 2)
    print(f"Время выполнения: {execution_time} секунд")
    return all_vacancies


def print_vacancies_by_company(vacancies_by_company):
    total_vacancies = 0
    for company_name, vacancies in vacancies_by_company.items():
        if len(vacancies) > 0:
            print(f"- {company_name}: {len(vacancies)} вакансий")
            total_vacancies += len(vacancies)
            for vacancy in vacancies:
                title = vacancy.get('name', 'Не указано')
                salary = vacancy.get('salary')
                if salary:
                    salary_from = salary.get('from', 'Не указано')
                    salary_to = salary.get('to', 'Не указано')
                else:
                    salary_from = 'Не указано'
                    salary_to = 'Не указано'
                url = vacancy.get('alternate_url', 'Нет ссылки')
                print(f"  - {title} - от {salary_from} до {salary_to}, Ссылка: {url}")
            print()
    print(f"\nВсего вакансий: {total_vacancies}")


def calculate_average_salary(vacancies_by_company):
    total_salary = 0
    salary_count = 0

    for vacancies in vacancies_by_company.values():
        for vacancy in vacancies:
            salary = vacancy.get('salary')
            if salary and (salary.get('from') or salary.get('to')):
                salary_from = salary.get('from', 0) or 0
                salary_to = salary.get('to', 0) or 0
                if salary_from and salary_to:
                    average = (salary_from + salary_to) / 2
                else:
                    average = salary_from if salary_from else salary_to
                total_salary += average
                salary_count += 1

    return total_salary / salary_count if salary_count > 0 else 0


def get_vacancies_with_higher_salary(vacancies_by_company, avg_salary):
    higher_salary_vacancies = {}
    for company_name, vacancies in vacancies_by_company.items():
        filtered_vacancies = [
            vacancy for vacancy in vacancies if (
                    vacancy.get('salary') and
                    (
                            (vacancy['salary'].get('from') or 0) +
                            (vacancy['salary'].get('to') or 0)
                    ) / 2 > avg_salary
            )
        ]
        if filtered_vacancies:
            higher_salary_vacancies[company_name] = filtered_vacancies
    return higher_salary_vacancies


def get_all_vacancies_from_api(company_id=None, pages=20):
    all_vacancies = []
    for page in range(pages):
        params = {'employer_id': company_id, 'page': page, 'per_page': 100} if company_id else {'page': page,
                                                                                                'per_page': 100}
        url = f"{BASE_URL}vacancies"
        response = requests.get(url, params=params)
        if response.status_code == 200:
            data = response.json()
            all_vacancies.extend(data.get('items', []))
            if len(data.get('items', [])) < 100:
                break
        else:
            print(f"Ошибка при получении данных о вакансиях: {response.status_code}")
            break

    return all_vacancies


def search_vacancies_by_keyword(vacancies_by_company, keyword):
    keyword = keyword.lower()
    matching_vacancies = {}
    total_matching_vacancies = 0
    for company_name, vacancies in vacancies_by_company.items():
        matching_vacancies[company_name] = [
            vacancy for vacancy in vacancies if keyword in vacancy.get('name', '').lower()
        ]
        total_matching_vacancies += len(matching_vacancies[company_name])

    for company_name, vacancies in matching_vacancies.items():
        if vacancies:
            print(f"\nКомпания: {company_name}")
            for vacancy in vacancies:
                title = vacancy.get('name', 'Не указано')
                salary = vacancy.get('salary')
                if salary:
                    salary_from = salary.get('from', 'Не указано')
                    salary_to = salary.get('to', 'Не указано')
                else:
                    salary_from = 'Не указано'
                    salary_to = 'Не указано'
                url = vacancy.get('alternate_url', 'Нет ссылки')
                print(f"  - {title} (от {salary_from} до {salary_to}), Ссылка: {url}")

    print(f"\nВсего вакансий, содержащих ключевое слово '{keyword}': {total_matching_vacancies}")
    print("\nРезультаты поиска завершены.")
