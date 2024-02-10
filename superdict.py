import fcntl
import mmap
import os
import pickle
from typing import Any


class SuperDict:
    def __init__(self, buffer_name: str = None, create_mode: bool = True, buffer_size: int = 1024) -> None:
        self.buffer_name = buffer_name
        self.buffer_size = buffer_size
        if create_mode:
            self.initialize_buffer()
        else:
            self.open_buffer()
        self.lock = Lock(self.buffer_fd)

    def initialize_buffer(self) -> None:
        self.buffer_fd = os.open(self.buffer_name, os.O_CREAT | os.O_RDWR)
        os.ftruncate(self.buffer_fd, self.buffer_size)
        self.buffer = mmap.mmap(self.buffer_fd, self.buffer_size)
        self.write_to_buffer({})

    def open_buffer(self) -> None:
        self.buffer_fd = os.open(self.buffer_name, os.O_RDWR)
        self.buffer = mmap.mmap(self.buffer_fd, self.buffer_size)

    def read_from_buffer(self) -> dict:
        return pickle.loads(self.buffer)

    def write_to_buffer(self, native_dict: dict) -> None:
        serialized_dict = pickle.dumps(native_dict)
        self.buffer[:len(serialized_dict)] = serialized_dict

    def __getitem__(self, key: str) -> tuple[Any, str]:
        return self.read_from_buffer()[key]

    def __setitem__(self, key: str, value: Any) -> None:
        native_dict = self.read_from_buffer()
        native_dict[key] = value
        self.write_to_buffer(native_dict)

    def __len__(self) -> int:
        return len(self.read_from_buffer())

    def __str__(self):
        return str(self.read_from_buffer())

    def close_buffer(self) -> None:
        if hasattr(self, "buffer"):
            self.buffer.close()
        if hasattr(self, "buffer_fd"):
            os.close(self.buffer_fd)

    def unlink_buffer(self) -> None:
        if hasattr(self, "buffer"):
            self.buffer.close()
            os.unlink(self.buffer_name)
        if hasattr(self, "buffer_fd"):
            os.close(self.buffer_fd)

    def __del__(self) -> None:
        self.close_buffer()


class Lock:
    def __init__(self, fd):
        self.fd = fd

    def __enter__(self):
        fcntl.lockf(self.fd, fcntl.LOCK_EX)

    def __exit__(self, exc_type, exc_val, exc_tb):
        fcntl.lockf(self.fd, fcntl.LOCK_UN)
