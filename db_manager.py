import requests
import psycopg2
from typing import Optional, Any, List
from concurrent.futures import ThreadPoolExecutor

BASE_URL = "https://api.hh.ru/"


class DBManager:
    def __init__(self, db_connection_params):
        self.connection = psycopg2.connect(**db_connection_params)
        self.cursor = self.connection.cursor()

    def insert_company(self, company_name: str, industry: Optional[str] = None, area: Optional[str] = None) -> int:
        query = "INSERT INTO companies (name, industry, area) VALUES (%s, %s, %s) ON CONFLICT (name) DO NOTHING RETURNING id"
        self.cursor.execute(query, (company_name, industry, area))
        self.connection.commit()
        result = self.cursor.fetchone()
        if result:
            return result[0]
        else:
            query = "SELECT id FROM companies WHERE name = %s"
            self.cursor.execute(query, (company_name,))
            return self.cursor.fetchone()[0]

    def insert_vacancy(self, company_id: int, title: str, salary_min: Optional[int], salary_max: Optional[int],
                       url: str):
        query = """
            INSERT INTO vacancies (title, salary_min, salary_max, url, company_id)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (url) DO NOTHING
        """
        self.cursor.execute(query, (title, salary_min, salary_max, url, company_id))
        self.connection.commit()

    def insert_vacancies_bulk(self, vacancies: List[tuple]):
        query = """
            INSERT INTO vacancies (title, salary_min, salary_max, url, company_id)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (url) DO NOTHING
        """
        self.cursor.executemany(query, vacancies)
        self.connection.commit()

    def get_companies_and_vacancies_count(self) -> List[tuple[Any, ...]]:
        query = """
            SELECT c.name, COUNT(v.id)
            FROM companies c
            LEFT JOIN vacancies v ON c.id = v.company_id
            GROUP BY c.id
            ORDER BY COUNT(v.id) DESC
            LIMIT 10
        """
        self.cursor.execute(query)
        return self.cursor.fetchall()

    def get_all_vacancies(self) -> List[tuple[Any, ...]]:
        query = """
            SELECT v.title, v.salary_min, v.salary_max, v.url, c.name
            FROM vacancies v
            JOIN companies c ON v.company_id = c.id
        """
        self.cursor.execute(query)
        return self.cursor.fetchall()

    def get_avg_salary(self) -> Optional[float]:
        query = """
            SELECT AVG((COALESCE(v.salary_min, 0) + COALESCE(v.salary_max, 0)) / 2.0) AS avg_salary
            FROM vacancies v
            WHERE v.salary_min IS NOT NULL OR v.salary_max IS NOT NULL
        """
        self.cursor.execute(query)
        return self.cursor.fetchone()[0]

    def get_vacancies_with_higher_salary(self) -> List[tuple[Any, ...]]:
        avg_salary = self.get_avg_salary()
        if avg_salary is None:
            return []

        query = """
            SELECT v.title, v.salary_min, v.salary_max, v.url, c.name
            FROM vacancies v
            JOIN companies c ON v.company_id = c.id
            WHERE ((COALESCE(v.salary_min, 0) + COALESCE(v.salary_max, 0)) / 2.0) > %s
        """
        self.cursor.execute(query, (avg_salary,))
        return self.cursor.fetchall()

    def get_vacancies_with_keyword(self, keyword: str) -> List[tuple[Any, ...]]:
        query = """
            SELECT v.title, v.salary_min, v.salary_max, v.url, c.name
            FROM vacancies v
            JOIN companies c ON v.company_id = c.id
            WHERE v.title ILIKE %s
        """
        self.cursor.execute(query, ('%' + keyword + '%',))
        return self.cursor.fetchall()

    def close_connection(self):
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()


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


def get_all_vacancies_for_companies(companies=None, db_manager=None):
    all_vacancies = {}
    with ThreadPoolExecutor(max_workers=10) as executor:
        if companies:
            futures = {executor.submit(get_vacancies_for_company, company['id'], 5): company for company in companies}
            for future in futures:
                company = futures[future]
                company_name = company['name']
                industry = company.get('industry', None)
                area = company.get('area', None)
                try:
                    vacancies = future.result()
                    if vacancies:
                        company_id = db_manager.insert_company(company_name, industry, area)
                        vacancies_data = []
                        for vacancy in vacancies:
                            title = vacancy.get('name', 'Не указано')
                            salary_info = vacancy.get('salary')
                            salary_min = salary_info.get('from') if salary_info else None
                            salary_max = salary_info.get('to') if salary_info else None
                            vacancy_url = vacancy.get('alternate_url', 'Нет ссылки')
                            vacancies_data.append((title, salary_min, salary_max, vacancy_url, company_id))
                            # Вывод вакансий в терминал
                            print(f"Вакансия: {title}, от {salary_min} до {salary_max}, Ссылка: {vacancy_url}")
                        db_manager.insert_vacancies_bulk(vacancies_data)
                except Exception as e:
                    print(f"Ошибка при получении данных для компании {company_name}: {e}")


if __name__ == "__main__":
    connection_params = {
        'dbname': 'postgres',
        'user': 'postgres',
        'password': 'zaziso00',
        'host': 'localhost',
    }

    db_manager = DBManager(connection_params)
    employers = [
        {'id': '1', 'name': 'Компания 1', 'industry': 'IT', 'area': 'Moscow'},
        {'id': '2', 'name': 'Компания 2', 'industry': 'Finance', 'area': 'Saint Petersburg'}
    ]

    get_all_vacancies_for_companies(employers, db_manager)
    db_manager.close_connection()
