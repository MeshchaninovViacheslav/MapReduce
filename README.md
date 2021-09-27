# Project Title

This is repository with ComputeGraph Homework solution in Yandex Data School 2021.

## Getting Started

These instructions will get you a copy of the project up and running on your local machine for development and testing
purposes. See deployment for notes on how to deploy the project on a live system.

## Prerequisites

Install compgraph library. You have to run these comand in your HW2 directory.

```bash
$ ~/.pyenv/versions/shad_2020/bin/pip install -e compgraph --force
```

## Интерфейс графа вычислений

Граф вычислений состоит из точек входа для данных и операций над ними.

Доступные операции описаны и реализованы в файле operations.py.

В файле algorithms.py можно посмотреть на примеры работы с данным графом.

Вот так может выглядеть граф, который подсчитывает кол-во слов в документах:

```python
graph = Graph.graph_from_iter('texts')
.map(operations.FilterPunctuation('text'))
.map(operations.LowerCase('text'))
.map(operations.Split('text'))
.sort(['text'])
.reduce(operations.Count('count'), ['text'])
.sort(['count', 'text'])
```

## Running the tests

Run test by following command:

```
# run test
pytest -vv
```

## Style check

```
flake8 --config ../setup.cfg compgraph/
```

### Break down into end to end tests

There are 3 group of test:

- graph tests
- operations tests
- algorithm tests

## Typing test

```
mypy compgraph/
```

## Contributing

This is not possible to contribute.

## Authors

- **Meshchaninov Viacheslav** -
  [Meshchaninov_Viacheslav](https://github.com/MeshchaninovViacheslav)
  
