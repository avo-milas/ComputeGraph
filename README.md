# ComputeGraph  

**ComputeGraph** is a library for creating computational graphs using **MapReduce** and **Join** structures, designed for data processing and analysis.  

### Basic Graph Architecture:  

> Input -> Map -> Sort -> Reduce -> Join -> Output  

- **Input**: Data input (from a file or generator)  
- **Map**: Transforming rows based on specified rules  
- **Sort**: Sorting data by keys  
- **Reduce**: Aggregating data by keys  
- **Join**: Merging data from multiple sources  
- **Output**: Execution result  

---

### **Installing**  

To install the library, run the following command:  

> `pip install -e compgraph --force-reinstal`  

---

## **Running the Tests**  

After installation, tests are available to verify the correctness and performance of the graph. To run the tests, execute:  

> `pytest compgraph`  

---

### **Tests Structure**  

- **Root test folder**: `test`  
- **Test categories**:  
  - **`tests/correctness`** - Validates the correctness of all algorithms and operations.  
  - **`tests/memory`** - Checks memory usage during implementation.  
  - **`tests/test_graph`**, **`tests/test_operations`** - Verifies the correctness of graph operations not covered by previous tests.  
  - **`tests/test_examples`** - Tests graph behavior on synthetic time-series data from files.  

---

## **Usage**  

Example scripts for solving specific tasks using the graph are provided in the `examples` folder:  

- **_Word Count_**: Counts the total occurrences of each word in the dataset.  
- **_TF-IDF_**: Calculates the top-3 documents for each word based on TF-IDF scores.  
- **_PMI_**: Finds the top-10 words most characteristic of each document using PMI metrics.  
- **_Yandex Maps_**: Computes average traffic speed in the city by hour and day of the week.  

---

### **Running Scripts**  

- **From the console**:  
> `python examples/run_word_count.py --input-file my_inp.txt --output-file my_output.txt`  

- **From an IDE**:  
  - Open the desired script, e.g., `run_word_count.py`  
  - Press **Run**  

---

### **Configuring Input and Output Data**  

- Specify the input data path using the `--input-file` flag in the console.  
- Set the output file path with the `--output-file` flag.  

For the **Yandex Maps** script, use these flags:  
> `--input-file-time`, `--input-file-len`, `--output-file`  

---

### **Using Generators for Input**  

To process data from a generator, set the dictionary key in the `input_filepath` passed to the `run` method:  

```python
input_filepath = 'docs'
graph = algorithms.word_count_graph(input_filepath, text_column='text', count_column='count')
result = graph.run(docs=lambda: iter(docs))
```

**Note**: By default, data is read from a generator. To read from a file, enable the corresponding flag when creating the graph in the script:  

```python
graph = word_count_graph(from_file=True)
```

---

## **Authors**  

- **Alina Salimova** -  
  [avo_milas](https://github.com/avo-milas)  
