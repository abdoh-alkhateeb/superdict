import pickle
from typing import Any
from multiprocessing.shared_memory import SharedMemory


class SuperDict:
    def __init__(self, shared_memory_name: str = None, create_mode: bool = True, buffer_size: int = 1024) -> None:
        self.shared_memory = SharedMemory(shared_memory_name, create_mode, buffer_size)
        self.initialize_buffer()

    def initialize_buffer(self) -> None:
        self.write_to_buffer({})

    def read_from_buffer(self) -> dict:
        return pickle.loads(self.shared_memory.buf.tobytes())

    def write_to_buffer(self, native_dict: dict) -> None:
        serialized_dict = pickle.dumps(native_dict)
        try:
            self.shared_memory.buf[:len(serialized_dict)] = serialized_dict
        except ValueError as e:
            raise Exception("Data can't fit in buffer") from e

    def __getitem__(self, key: str) -> Any:
        return self.read_from_buffer()[key]

    def __setitem__(self, key: str, value: Any) -> None:
        native_dict = self.read_from_buffer()
        native_dict[key] = value
        self.write_to_buffer(native_dict)

    def __len__(self) -> int:
        return len(self.read_from_buffer())

    def __str__(self):
        return str(self.read_from_buffer())

    def close_shared_memory(self) -> None:
        if not hasattr(self, "shared_memory"):
            return
        if self.shared_memory is not None:
            self.shared_memory.close()

    def unlink_shared_memory(self) -> None:
        if not hasattr(self, "shared_memory"):
            return
        if self.shared_memory is not None:
            self.shared_memory.unlink()
            self.shared_memory = None

    def __del__(self) -> None:
        self.close_shared_memory()
