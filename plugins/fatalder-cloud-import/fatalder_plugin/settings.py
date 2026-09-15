"""Declarative settings consumed by the neomega configuration manager."""
from dataclasses import dataclass, field
from pathlib import PurePath
from urllib.parse import urlsplit
from uuid import UUID


@dataclass(frozen=True)
class RentalAccess:
    passcode: str = field(metadata={'title': '租赁服密码', 'secret': True})


@dataclass(frozen=True)
class Settings:
    worker_url: str = field(metadata={'title': 'Worker HTTPS 地址'})
    api_key: str = field(metadata={'title': 'Worker API Key', 'secret': True})
    target_server_id: str = field(metadata={'title': 'Worker 目标配置 ID'})
    rental_server_code: str = field(metadata={'title': '租赁服号'})
    admin_uuids: list[str] = field(metadata={'title': '管理员 UUID 白名单'})
    rental_access: RentalAccess | None = field(default=None, metadata={'title': '有密码的租赁服访问配置'})
    files_directory: str = field(default='imports', metadata={'title': '数据目录内的建筑目录'})
    revoke_operator_on_completion: bool = field(default=False, metadata={
        'title': '导入结束后撤销插件授予的 OP',
        'description': '默认保留 OP；开启后须确认恢复原权限才结束权限会话。'})
    poll_seconds: float = field(default=2.0, metadata={'minimum': 1, 'maximum': 30})
    max_upload_bytes: int = field(default=104857600, metadata={'minimum': 1, 'maximum': 1073741824})

    def __post_init__(self):
        url = urlsplit(self.worker_url)
        if (url.scheme != 'https' or not url.hostname or url.username or url.password
                or url.path not in ('', '/') or url.query or url.fragment):
            raise ValueError('invalid Worker address')
        if not self.target_server_id or not self.rental_server_code.isdecimal():
            raise ValueError('invalid target')
        if not self.admin_uuids or any(str(UUID(v)) != v for v in self.admin_uuids):
            raise ValueError('invalid administrators')
        path = PurePath(self.files_directory)
        if path.is_absolute() or '..' in path.parts or not path.parts:
            raise ValueError('invalid files directory')
