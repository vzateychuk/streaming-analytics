## Docker

Connect to MariaDB from the MySQL/MariaDB command line client

The following command starts another mariadb container instance and runs the mariadb command line client against your original mariadb container, allowing you to execute SQL statements against your database instance:

$ docker run -it --network some-network --rm mariadb mariadb -hsome-mariadb -uexample-user -p
... where some-mariadb is the name of your original mariadb container (connected to the some-network Docker network).