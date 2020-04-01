from lib.tushare import tushare


class Job:
    """
    定义任务执行的模板方法
    """

    """
    调用接口，同步数据
    """
    def sync(self, **kwargs):
        sync_ts_svc = self.init_sync_ts_svc(**kwargs)
        #  调用接口同步数据
        for sync_svc in sync_ts_svc:
            tushare.download_data(**sync_svc)

    """
    初始化需要同步的任务清单
    """
    def init_sync_ts_svc(self, **kwargs) -> dict:
        pass