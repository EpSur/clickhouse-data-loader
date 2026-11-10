import pandas as pd
import os
from datetime import datetime, timedelta
import random

def create_test_file(path="data/test.xlsx", rows=10):
    # Создаем папку, если её нет
    os.makedirs(os.path.dirname(path), exist_ok=True)
    
    # Генерируем фейковые данные
    data = {
        'name1': [f"item_{i}" for i in range(rows)],
        'name2': [random.randint(100, 999) for _ in range(rows)],
        'name3': [datetime.now() - timedelta(days=i) for i in range(rows)]
    }
    
    df = pd.DataFrame(data)
    df.to_excel(path, index=False)
    print(f"✅ Тестовый файл успешно создан по пути: {path}")

if __name__ == "__main__":
    create_test_file()
