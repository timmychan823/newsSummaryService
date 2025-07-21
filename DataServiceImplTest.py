
from src.service.DataReleaseImpl import DataReleaseServiceImpl
import asyncio

if __name__ == "__main__":
    temp = DataReleaseServiceImpl()
    asyncio.run(temp.getDataRelease())