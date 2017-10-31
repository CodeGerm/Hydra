Hydra
================

Hydra is a project that can be used as a flume (https://flume.apache.org/) source to do database copy/snapshot in streaming nature. It's main features:
- Concurrently read data from multiple tables, persist the uploading status in files/events
- Flexible structure which can support a variety of input/output formats
- A task based trigger condition which allows invoking source programmatically

## Strcture

## Trigger condition
There are two sources: <b>SqlEventDrivenSource</b> and <b>SqlSource</b> and two running mode: <b>schedule</b> and <b>task</b>. SqlEventDrivenSource only supports task mode.
-Schedule Mode:
-Task Mode:
## Configuration
----------
Mandatory properties in <b>bold</b>
### Source level configurations
| Property Name | Default | Description |
| ----------------------- | :-----: | :---------- |
| <b>channels</b> | - | Connected channel names |
| <b>type</b> | - | The component type name: com.github.codegerm.hydra.source.SqlEventDrivenSource or com.github.codegerm.hydra.source.SqlSource |
| timeout | 100000 | timeout (milleseconds) for the main task to finish, unfinished handler will be killed |

### Handler level configurations (hibernate handler)
| Property Name | Default | Description |
| ----------------------- | :-----: | :---------- |
| <b>hibernate.connection.url</b> | - | Url to connect with the remote Database |
| <b>hibernate.connection.user</b> | - | Username to connect with the database |
| <b>hibernate.connection.password</b> | - | Password to connect with the database |
| <b>status.file.name</b> | - | Local file name to save last row number read |
| table | - | Table to export data (Not used in task mode) |
| columns.to.select | * | Which colums of the table will be selected ((Not used in task mode))|
| hibernate.connection.driver_class | -| Driver class to use by hibernate, if not specified the framework will auto asign one |
| status.file.path | /var/lib/flume | Path to save the status file |
| start.from | 0 | Start value to import data |
| hibernate.dialect | - | Dialect to use by hibernate, if not specified the framework will auto asign one. Check https://docs.jboss.org/hibernate/orm/4.3/manual/en-US/html/ch03.html#configuration-optional-dialects for a complete list of available dialects |
| hibernate.connection.provider_class | - | Set to org.hibernate.connection.C3P0ConnectionProvider to use C3P0 connection pool (recommended for production) |
| hibernate.c3p0.min_size | - | Min connection pool size |
| hibernate.c3p0.max_size | - | Max connection pool size |
| delimiter.entry | , | delimiter of incoming entry |
| batch.size| 100 | Batch size to send events to flume channel |
| read.only | false| Sets read only session with DDBB |
