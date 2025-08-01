### MemcLoad

*Задание*: 
Создаем простого демона на Go, проводим сравнение с аналогичным на Python.
Описание/Пошаговая инструкция выполнения домашнего задания:
В этом домашнем задании предлагается переписать конкурентный memcache loader, реализованный на Python в одном из прошлых заданий, на Golang, соблюдая при этом идиоматику языка и используя его возможности, рассмотренные на занятии

go_multithreading.go - скрипт на go (переписанный из прошлого задания memc_load_multithreading.py) 

[//]: # (Результаты выполнения одднопоточного и многопоточного скрипта)
*Тестирование* скриптов производилось на 20170929000000.tsv


[//]: # (Инициализация модуля go)
* go mod init go_multithreading

[//]: # (Установка зависимостей)
* Уставновка пакета go1.24.5.darwin-arm64 (для  MacOS)
* Установка зависимостей для go
 - go get github.com/bradfitz/gomemcache/memcache
 - go get github.com/golang/protobuf/proto

[//]: # (Сборка)
* go build -o go_multithreading
 
[//]: # (Запуск)
* ./go_multithreading --pattern="/sample/*.tsv.gz"  --dry=false

[//]: # (Запуск сервера memcache)
* memcached -p 33016 -U 0 -vv