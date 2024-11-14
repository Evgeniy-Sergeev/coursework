from concurrent.futures import ThreadPoolExecutor
from hh_api import get_all_vacancies_for_companies, print_vacancies_by_company, get_vacancies_with_higher_salary


def user_interface():
    with ThreadPoolExecutor() as executor:
        future_vacancies = executor.submit(get_all_vacancies_for_companies)
        vacancies_by_company = future_vacancies.result()

    while True:
        print("\nВыберите действие:")
        print("1: Посмотреть количество вакансий по компаниям")
        print("2: Посмотреть все вакансии")
        print("3: Средняя заработная плата")
        print("4: Вакансии с зарплатой выше средней")
        print("5: Поиск вакансий по ключевому слову")
        print("6: Выйти")

        try:
            choice = int(input("\nВведите номер действия (1-6): "))
        except ValueError:
            print("Ошибка ввода! Пожалуйста, введите цифру от 1 до 6.")
            continue

        if choice == 1:
            print("\nКоличество вакансий по компаниям:")
            total_vacancies = 0
            for company, vacancies in vacancies_by_company.items():
                num_vacancies = len(vacancies)
                total_vacancies += num_vacancies
                print(f"\n- {company}: {num_vacancies} вакансий")
                for vacancy in vacancies:
                    title = vacancy.get('name', 'Не указано')
                    url = vacancy.get('alternate_url', 'Нет ссылки')
                    print(f"  - {title}, Ссылка: {url}")

            print(f"\nВсего вакансий: {total_vacancies}")

        elif choice == 2:
            print("\nВсе вакансии:")
            for company, vacancies in vacancies_by_company.items():
                print(f"\nКомпания: {company}")
                for vacancy in vacancies:
                    title = vacancy.get('name', 'Не указано')
                    url = vacancy.get('alternate_url', 'Нет ссылки')
                    print(f"  - {title}, Ссылка: {url}")

        elif choice == 3:
            salaries = []
            for company_vacancies in vacancies_by_company.values():
                for vacancy in company_vacancies:
                    salary = vacancy.get('salary')
                    if salary:
                        salary_from = salary.get('from', 0) or 0
                        salary_to = salary.get('to', 0) or 0
                        avg = (salary_from + salary_to) / 2
                        if avg > 0:
                            salaries.append(avg)
            if salaries:
                avg_salary = sum(salaries) / len(salaries)
                print(f"\nСредняя заработная плата: {avg_salary:.2f} RUB")
            else:
                print("\nНет данных для расчета средней зарплаты.")

        elif choice == 4:
            salaries = []
            for company_vacancies in vacancies_by_company.values():
                for vacancy in company_vacancies:
                    salary = vacancy.get('salary')
                    if salary:
                        salary_from = salary.get('from', 0) or 0
                        salary_to = salary.get('to', 0) or 0
                        avg = (salary_from + salary_to) / 2
                        if avg > 0:
                            salaries.append(avg)
            if salaries:
                avg_salary = sum(salaries) / len(salaries)
                print(f"\nСредняя заработная плата: {avg_salary:.2f} RUB")
                higher_salary_vacancies = get_vacancies_with_higher_salary(vacancies_by_company, avg_salary)
                print_vacancies_by_company(higher_salary_vacancies)
            else:
                print("\nНет данных для расчета средней зарплаты.")

        elif choice == 5:
            keyword = input("\nВведите ключевое слово для поиска вакансий: ").lower()
            print(f"\nРезультаты поиска вакансий по слову '{keyword}':")
            for company, vacancies in vacancies_by_company.items():
                matching_vacancies = [
                    vacancy for vacancy in vacancies if keyword in vacancy.get('name', '').lower()
                ]
                if matching_vacancies:
                    print(f"\nКомпания: {company}")
                    for vacancy in matching_vacancies:
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

        elif choice == 6:
            print("Выход из программы...")
            break

        else:
            print("Ошибка ввода! Пожалуйста, введите цифру от 1 до 6.")


if __name__ == "__main__":
    user_interface()
