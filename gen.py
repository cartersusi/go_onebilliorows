import numpy as np
import pandas as pd
import gc

tmp = pd.read_csv('worldcities.csv')
cities = []

for i, city in enumerate(tmp['city'].values.tolist()):
    if i % 2 == 0:
        cities.append(city)

total_rows = 1_000_000_000
chunk_size = 100_000_000

output_file = 'large_dataset.txt'
with open(output_file, 'w') as f:
    for _ in range(total_rows // chunk_size):
        city_indices = np.random.randint(0, len(cities), size=chunk_size)
        selected_cities = np.array(cities)[city_indices]
        temperatures = np.random.uniform(0.0, 40.0, size=chunk_size)

        buffer = [f"{city};{temp:.1f}\n" for city, temp in zip(selected_cities, temperatures)]
        f.writelines(buffer)

        del city_indices, selected_cities, temperatures, buffer
        gc.collect()

print(f"Dataset generated and saved to {output_file}")
