"""
启动VeighNa Trader UI界面
用于配置数据库、数据源等全局设置
"""
from vnpy.event import EventEngine
from vnpy.trader.engine import MainEngine
from vnpy.trader.ui import MainWindow, create_qapp

# 如果需要添加网关或应用，取消下面的注释并安装对应的模块
# from vnpy_ctp import CtpGateway
# from vnpy_ctastrategy import CtaStrategyApp
# from vnpy_datamanager import DataManagerApp


def main():
    """启动VeighNa Trader UI"""
    # 创建Qt应用
    qapp = create_qapp()
    
    # 创建事件引擎
    event_engine = EventEngine()
    
    # 创建主引擎
    main_engine = MainEngine(event_engine)
    
    # 添加网关（如果需要）
    # main_engine.add_gateway(CtpGateway)
    
    # 添加应用（如果需要）
    # main_engine.add_app(CtaStrategyApp)
    # main_engine.add_app(DataManagerApp)
    
    # 创建主窗口
    main_window = MainWindow(main_engine, event_engine)
    main_window.showMaximized()
    
    # 运行应用
    qapp.exec()


if __name__ == "__main__":
    main()

