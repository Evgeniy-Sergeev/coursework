import psycopg2
from typing import List, Tuple, Optional
import configparser


class DBManager:
    def __init__(self, db_params: dict):
        self.connection = psycopg2.connect(**db_params)
        self.cursor = self.connection.cursor()

    def create_tables(self):
        """Создание таблиц для компаний и вакансий"""
        create_companies_table = """
        CREATE TABLE IF NOT EXISTS companies (
            id SERIAL PRIMARY KEY,
            name TEXT UNIQUE NOT NULL
        );
        """
        create_vacancies_table = """
        CREATE TABLE IF NOT EXISTS vacancies (
            id SERIAL PRIMARY KEY,
            title TEXT NOT NULL,
            salary_min INTEGER,
            salary_max INTEGER,
            company_id INTEGER,
            url TEXT,
            FOREIGN KEY (company_id) REFERENCES companies(id)
        );
        """
        self.cursor.execute(create_companies_table)
        self.cursor.execute(create_vacancies_table)
        self.connection.commit()

    def insert_company(self, company_name: str) -> int:
        """Вставка компании в таблицу"""
        insert_query = "INSERT INTO companies (name) VALUES (%s) ON CONFLICT (name) DO NOTHING RETURNING id;"
        self.cursor.execute(insert_query, (company_name,))
        self.connection.commit()
        company_id = self.cursor.fetchone()
        return company_id[0] if company_id else None

    def insert_vacancies_bulk(self, vacancy_records: List[Tuple]):
        """Вставка множества вакансий в таблицу"""
        insert_query = """
        INSERT INTO vacancies (title, salary_min, salary_max, url, company_id) 
        VALUES (%s, %s, %s, %s, %s);
        """
        self.cursor.executemany(insert_query, vacancy_records)
        self.connection.commit()

    def get_all_vacancies(self):
        """Получение всех вакансий с данными о компании"""
        self.cursor.execute("""
        SELECT vacancies.title, vacancies.salary_min, vacancies.salary_max, vacancies.url, companies.name
        FROM vacancies
        JOIN companies ON vacancies.company_id = companies.id;
        """)
        return self.cursor.fetchall()

    def get_companies_and_vacancies_count(self):
        """Получение количества вакансий для каждой компании"""
        self.cursor.execute("""
        SELECT companies.name, COUNT(vacancies.id)
        FROM companies
        LEFT JOIN vacancies ON companies.id = vacancies.company_id
        GROUP BY companies.name;
        """)
        return self.cursor.fetchall()

    def get_avg_salary(self) -> Optional[float]:
        """Получение средней зарплаты по вакансиям"""
        self.cursor.execute("""
        SELECT AVG(salary_min + salary_max) / 2
        FROM vacancies
        WHERE salary_min IS NOT NULL AND salary_max IS NOT NULL;
        """)
        result = self.cursor.fetchone()
        return result[0] if result else None

    def get_vacancies_with_keyword(self, keyword: str):
        """Поиск вакансий по ключевому слову"""
        self.cursor.execute("""
        SELECT vacancies.title, vacancies.salary_min, vacancies.salary_max, vacancies.url, companies.name
        FROM vacancies
        JOIN companies ON vacancies.company_id = companies.id
        WHERE vacancies.title ILIKE %s;
        """, (f"%{keyword}%",))
        return self.cursor.fetchall()

    def get_vacancies_with_higher_salary(self, avg_salary: float):
        """Поиск вакансий с зарплатой выше средней"""
        self.cursor.execute("""
        SELECT vacancies.title, vacancies.salary_min, vacancies.salary_max, vacancies.url, companies.name
        FROM vacancies
        JOIN companies ON vacancies.company_id = companies.id
        WHERE (vacancies.salary_min + vacancies.salary_max) / 2 > %s;
        """, (avg_salary,))
        return self.cursor.fetchall()

    def close_connection(self):
        """Закрытие соединения с базой данных"""
        self.cursor.close()
        self.connection.close()
