import pandas as pd # type: ignore
import random

class CsvDataProcessor:
    """
    Класс для обработки данных из CSV файлов.
    Позволяет читать, изменять и сохранять CSV файлы.
    """
    
    def __init__(self, file_path):
        """
        Конструктор класса.
        
        Args:
            file_path: Путь к CSV файлу
        """
        self.file_path = file_path
        self.data = None
        
    def load_data(self):
        """
        Загружает данные из CSV файла.
        """
        # При необходимости можно добавить дополнительные параметры:
        # sep=',' - разделитель полей (по умолчанию запятая)
        # encoding='utf-8' - кодировка файла
        # header=0 - номер строки с заголовками (0 - первая строка)
        self.data = pd.read_csv(self.file_path)
        return self.data
    
    def convert_state_to_column(self, state_dict):
        """
        Преобразует словарь с распределением индексов строк по значениям
        в список значений для добавления в DataFrame.
        
        Args:
            state_dict: Словарь вида {значение: [индексы_строк]}
            
        Returns:
            list: Список значений для всех строк DataFrame
        """
        # Создаем список с дефолтным значением 0 для всех строк
        column_values = [0] * len(self.data)
        
        # Заполняем значения для соответствующих индексов
        for key, indices in state_dict.items():
            for index in indices:
                column_values[index] = key
        
        return column_values
    
    def add_template_column(self, column_name):
        """
        Создаёт новую колонку в данных и заполняет её с помощью шаблонной функции.
        
        Args:
            column_name: Название новой колонки
        
        Returns:
            DataFrame с добавленной колонкой
        """
        if self.data is None:
            self.load_data()
        
        # Получаем новое распределение значений
        new_state = self.template_function()
        
        # Преобразуем словарь состояния в список значений для колонки
        new_column_values = self.convert_state_to_column(new_state)
        
        # Добавляем новую колонку к данным
        self.data[column_name] = new_column_values
        
        print(f"Колонка '{column_name}' успешно добавлена.")
        return self.data
    
    def template_function(self):
        """
        Создает новое распределение студентов по станциям, 
        минимизируя пересечения с предыдущим распределением.
        
        Returns:
            dict: Словарь с новым распределением {станция: [номера_студентов]}
        """
        previous_state = {i: [] for i in range(0, 9)}
        new_state = {i: [] for i in range(0, 9)}
        
        # Получаем и обрабатываем данные из последней колонки
        self._collect_previous_state(previous_state)
        
        # Распределяем студентов по новым станциям
        self._distribute_students(previous_state, new_state)
        
        print(f"Новое состояние: {new_state}")
        return new_state
    
    def _collect_previous_state(self, previous_state):
        """
        Заполняет previous_state данными из последней колонки.
        
        Args:
            previous_state: Словарь для заполнения
        """
        last_column_name = self.data.columns[-1]
        
        for i, value in enumerate(self.data[last_column_name]):
            try:
                if pd.isnull(value) or int(value) == 0:
                    key = 0
                else:
                    key = int(value)
                
                if key in previous_state:
                    previous_state[key].append(i)
                    
            except (ValueError, TypeError):
                print(f"Предупреждение: Значение '{value}' в строке {i} не является числом от 1 до 8")
    
    def _distribute_students(self, previous_state, new_state):
        """
        Распределяет студентов из previous_state в new_state.
        
        Args:
            previous_state: Исходное состояние
            new_state: Новое состояние для заполнения
        """
        for key, students in previous_state.items():
            if not students:
                continue  # Пропускаем пустые списки
                
            print(f"Ключ: {key}, Индексы: {students}")
            
            if key != 0:
                # Назначаем станции с минимизацией пересечений
                for student in students:
                    try:
                        assigned_station = self._assign_station_for_student(student, students, new_state)
                        new_state[assigned_station].append(student)
                        print(f"Студент {student} назначен на станцию {assigned_station}")
                    except ValueError:
                        print(f"Ошибка: Не удалось назначить станцию для студента {student}")
            else:
                # Для строк с нулевыми значениями просто распределяем равномерно
                for student in students:
                    assigned_station = self.find_random_available(new_state)
                    new_state[assigned_station].append(student)
                    print(f"Студент {student} назначен на станцию {assigned_station}")
    
    def _assign_station_for_student(self, student, students_group, new_state):
        """
        Находит подходящую станцию для студента.
        
        Args:
            student: ID студента
            students_group: Группа, из которой происходит студент
            new_state: Текущее состояние распределения
            
        Returns:
            int: Номер станции
        """
        assigned_station = self.find_random_available(new_state)
        
        # Пытаемся найти станцию без пересечений
        max_attempts = 15
        attempts = 0
        
        while not self.check_not_in_list(students_group, new_state[assigned_station]) and attempts < max_attempts:
            assigned_station = self.find_random_available(new_state)
            attempts += 1
        
        return assigned_station

    def randomize(self):
        return random.randint(1, 8)
    
    def find_random_available(self, lst):
        """
        Находит случайную станцию, которая не заполнена.
        
        Args:
            lst: Словарь с распределением {станция: [студенты]}
        
        Returns:
            int: Номер доступной станции
            
        Raises:
            ValueError: Если все станции заполнены
        """
        # Проверяем, есть ли хотя бы одна незаполненная станция
        all_full = True
        for station in range(1, 9):
            if len(lst[station]) < 3:
                all_full = False
                break
        
        # Если все станции заполнены, вызываем исключение
        if all_full:
            raise ValueError("Все станции заполнены до максимума")
        
        # Находим случайную незаполненную станцию
        max_attempts = 8  # Защита от слишком долгого поиска
        attempts = 0
        assigned_station = self.randomize()
        while len(lst[assigned_station]) == 3 and attempts < max_attempts:
            assigned_station = self.randomize()
            attempts += 1
        
        # Если не нашли случайным образом, найдем первую доступную
        if len(lst[assigned_station]) == 3:
            for station in range(1, 9):
                if len(lst[station]) < 3:
                    assigned_station = station
                    break
        
        return assigned_station
    
    def check_not_in_list(self, values, lst):
        """
        Проверяет, есть ли хотя бы одно значение из списка values в списке lst.
        
        Args:
            values: Список значений для проверки
            lst: Список, в котором нужно проверить наличие значений
            
        Returns:
            bool: True, если ни одно значение не найдено в списке, иначе False
        """
        for value in values:
            if value in lst:
                return False
        return True
    
    def save_data(self, output_file_path=None):
        """
        Сохраняет данные в CSV файл.
        
        Args:
            output_file_path: Путь для сохранения файла CSV (если None, перезаписывает исходный файл)
        """
        if self.data is None:
            print("Нет данных для сохранения.")
            return False
            
        # Если путь не указан, используем исходный путь
        if output_file_path is None:
            output_file_path = self.file_path
        
        # Сохраняем DataFrame в CSV файл
        self.data.to_csv(output_file_path, index=False, float_format='%d')
        
        print(f"Данные успешно сохранены в файл: {output_file_path}")
        return True


if __name__ == "__main__":
    processor = CsvDataProcessor('/Users/a1/Projects/ta/TA/Data/result.csv')
    data = processor.load_data()
    
    initial_cols = processor.data.shape[1]

    # Добавляем 8 новых колонок с шаблоном
    for i in range(3, 9):
        name = f"Lab_{i}"
        processor.add_template_column(name)
    # Сохраняем результат в новый файл
    processor.save_data('/Users/a1/Projects/ta/TA/Data/106_full_.csv')