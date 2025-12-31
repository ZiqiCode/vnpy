# 安装 TA-Lib 指南

## Windows 安装 TA-Lib

### 方法1：使用预编译的 wheel 文件（推荐）

1. 下载对应 Python 版本的 wheel 文件：
   - 访问：https://www.lfd.uci.edu/~gohlke/pythonlibs/#ta-lib
   - 下载对应 Python 版本的 `.whl` 文件（如：TA_Lib‑0.4.28‑cp311‑cp311‑win_amd64.whl）

2. 安装：
   ```bash
   pip install TA_Lib‑0.4.28‑cp311‑cp311‑win_amd64.whl
   ```

### 方法2：使用 conda（如果 conda 环境正常）

```bash
conda install -c conda-forge ta-lib
```

### 方法3：手动编译（高级用户）

1. 下载 TA-Lib C 库：https://ta-lib.org/install/
2. 解压到 `C:\ta-lib`
3. 设置环境变量：
   ```bash
   set TA_INCLUDE_PATH=C:\ta-lib\include
   set TA_LIBRARY_PATH=C:\ta-lib\lib
   ```
4. 安装 Python 包：
   ```bash
   pip install TA-Lib
   ```

## 临时解决方案

如果暂时无法安装 TA-Lib，可以修改 `vnpy/trader/utility.py`，将：
```python
import talib
```
改为：
```python
try:
    import talib
except ImportError:
    talib = None
```

然后在需要使用 talib 的地方添加检查。

