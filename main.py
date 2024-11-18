import configparser
from hh_api import get_vacancies_for_company
from db_manager import DBManager

# Чтение конфигурации базы данных
config = configparser.ConfigParser()
config.read('database.ini')

db_params = {
    'dbname': config['database']['dbname'],
    'user': config['database']['user'],
    'password': config['database']['password'],
    'host': config['database']['host']
}

# Создание объекта DBManager и таблиц
db_manager = DBManager(db_params)
db_manager.create_tables()

# Получение и сохранение данных о вакансиях
vacancies = get_vacancies_for_company(pages=5)  # Увеличьте pages, если хотите больше данных

for vacancy in vacancies:
    company_name = vacancy.get('employer', {}).get('name', 'Неизвестная компания')
    vacancy_title = vacancy.get('name', 'Не указано')
    salary_details = vacancy.get('salary')
    salary_min = salary_details.get('from') if salary_details else None
    salary_max = salary_details.get('to') if salary_details else None
    vacancy_url = vacancy.get('alternate_url', 'Нет ссылки')

    # Вставка данных о компании в базу данных
    company_id = db_manager.insert_company(company_name)

    # Вставка вакансий
    vacancy_records = [(vacancy_title, salary_min, salary_max, vacancy_url, company_id)]
    db_manager.insert_vacancies_bulk(vacancy_records)

# Интерфейс для взаимодействия с пользователем
while True:
    print("\nВыберите действие:")
    print("1: Все вакансии")
    print("2: Вакансии по компаниям")
    print("3: Средняя зарплата")
    print("4: Вакансии по ключевому слову")
    print("5: Вакансии с зарплатой выше средней")
    print("6: Выйти")

    choice = input("Введите номер действия: ")

    if choice == '1':
        for vacancy in db_manager.get_all_vacancies():
            print(
                f"Вакансия: {vacancy[0]}, От {vacancy[1]} до {vacancy[2]}, Ссылка: {vacancy[3]}, Компания: {vacancy[4]}")
    elif choice == '2':
        for company in db_manager.get_companies_and_vacancies_count():
            print(f"Компания: {company[0]}, Количество вакансий: {company[1]}")
    elif choice == '3':
        print(f"Средняя зарплата: {db_manager.get_avg_salary():.2f}")
    elif choice == '4':
        keyword = input("Введите ключевое слово: ")
        for vacancy in db_manager.get_vacancies_with_keyword(keyword):
            print(
                f"Вакансия: {vacancy[0]}, От {vacancy[1]} до {vacancy[2]}, Ссылка: {vacancy[3]}, Компания: {vacancy[4]}")
    elif choice == '5':
        avg_salary = db_manager.get_avg_salary()
        for vacancy in db_manager.get_vacancies_with_higher_salary(avg_salary):
            print(
                f"Вакансия: {vacancy[0]}, От {vacancy[1]} до {vacancy[2]}, Ссылка: {vacancy[3]}, Компания: {vacancy[4]}")
    elif choice == '6':
        db_manager.close_connection()
        break
    else:
        print("Неверный выбор.")
