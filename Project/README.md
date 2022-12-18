# SQL проект по созданию ETL процесса

## Задача
Разработать ETL процесс, получающий выгрузку данных, загружающий ее в хранилище данных и ежедневно строящий отчет.

## Порядок работы
1. Создание таблиц
2. Загрузка таблиц из sql файла ddl_dml.sql
3. Загрузка транзакции за текущий день
4. Загрузка список терминалов на текущий день (загрузка SCD Type 2)
5. Поиск мошеннических операций при просроченном паспорте.
6. Выводится витрина данных с мошенническими операциями.

!