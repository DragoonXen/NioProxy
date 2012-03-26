1. Для сборки проекта нужен установленный maven. Использовался maven из стандартного репозитория debian. Комманда: sudo apt-get install maven2;
2. При сборке проекта с помощью скрипта build.sh исполняемый jar файл вместе с файлом настройки будет лежать в папке bin/;
3. При сборке вручную можно использовать комманды mvn clean package [-Pinfo (default) | -Prelease | -Pdebug ], задаёт уровень логирования log4j. Файлы *.jar и proxy.properties будут находиться в папке target/, использовать jar-with-dependencies;
4. Комманда запуска: java -jar [path_to_jar_file_with_dependencies];
5. proxy.properties должен находиться в рабочей директории.
