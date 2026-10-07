# neomega_database 1.0.0

安装专属 SQLite 与可选 MySQL 的同步 DB-API 适配库。消费插件在 release.json 的 libraries 中声明 neomega_database，通过 `import neomega_database` 使用。普通键值业务继续使用 ctx.storage；这个库不读取 Host 内部数据库。

`sqlite(ctx, name='application', timeout=5)` 仅在 `ctx.data_dir/databases/<name>.sqlite3` 建库；名称不能包含路径。启用外键和 WAL。`mysql(ctx, host=..., user=..., database=..., password_ref='secret:database', port=3306, timeout=5, ssl=...)` 通过 `ctx.secrets` 读取安装私密凭据，支持 TLS 参数和连接/读/写超时。

MySQL 需要消费插件发行包明确锁定并携带 PyMySQL（或注入遵循 pymysql.connect 签名的 connector），本库不会启动时 pip 安装。MySQL 账号应只拥有本安装 schema 权限；schema 名本身不是权限隔离。凭据只存私密 secret 文件，禁止写入配置、README、日志或 ZIP。轮换凭据后关闭并重建连接。Python 同 UID 插件不是安全沙箱。

用 `with db.transaction() as tx:` 管理事务，正常退出提交，异常回滚。SQLite 用 `?`，MySQL 用 `%s` 参数占位；参数单独传给 execute/executemany，不拼接数据进 SQL。查询通过 fetchone/fetchmany 读取。嵌套事务明确拒绝。DDL、迁移及 MySQL 隐式提交语句由消费插件管理，事务包装不能让 MySQL DDL 可回滚。

同步 SQL 不能直接放进异步事件循环；通过消费插件拥有的阻塞任务执行，在 on_stop 先等待任务退出、再 close。每个对象串行事务，跨安装不复用连接。断线或提交超时不自动重试写入；调用者必须按自身业务幂等记录核对实际结果。不要声称 SQL 与 Host storage/世界动作具备原子性。

许可证 AGPL-3.0-only。example.py 是消费示例。
