# Databricks notebook source
class DataLoader():
    def __init__(self):
        self.catalog = "dev"
        self.db = "spark_db"
        self.volume_name = "datasets"
        self.owner = "Albertbilu43"          #"LearningJournal"
        self.repo = "scholarnest_datasets"

    def cleanup_dir(self, dest_path): 
        print(f"Cleaning {dest_path}...", end='')
        dbutils.fs.rm(dest_path, recurse=True)  # Borra la carpeta y todo su contenido de forma recursiva.
        dbutils.fs.mkdirs(dest_path)            # Vuelve a crear la carpeta vacía inmediatamente.
        print("Done")

    def clean_ingest_data(self, source, dest):
        import requests     # To use the Requests library in Python, you must first pip install it
                            # you can import the module and start making HTTP requests
        from concurrent.futures import ThreadPoolExecutor #  provides a high-level interface for asynchronously executing tasks using a pool of worker threads. It is primarily used to speed up I/O-bound tasks—such as web scraping, database queries, or file operations—that can take a long time to complete.
        import collections

        dest_path = f"/Volumes/{self.catalog}/{self.db}/{self.volume_name}/{dest}" # ingest_data file location "/Volumes/dev/spark_db/datasets/ingest_data"
        self.cleanup_dir(dest_path) # Executes the function that cleans up and recreates the destination folder
        
        # https://api.github.com/repos/Albertbilu43/scholarnest_datasets/contents/datasets/ingest_data
        api_url = f"https://api.github.com/repos/{self.owner}/{self.repo}/contents/{source}" # The above path is the URL of the GitHub repository that contains the files to be downloaded. 
        files = requests.get(api_url).json()
        download_urls = [file["download_url"] for file in files]

        def download_files(download_url):
            filename = download_url.split("/")[-1]
            with requests.get(download_url, stream=True) as r:                
                with open(f"{dest_path}/{filename}", "wb") as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        f.write(chunk)               

        print(f"Downloading files...", end='')
        with ThreadPoolExecutor(max_workers=2) as executors:
            collections.deque(executors.map(download_files, download_urls))
        print("Done")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ###### def cleanup_dir(self, dest_path):
# MAGIC Este fragmento de código es una función típica para
# MAGIC Databricks (usando dbutils) que se encarga de "vaciar" y "recrear" un directorio antes de realizar un proceso de escritura.
# MAGIC Aquí tienes un desglose rápido de lo que hace y una pequeña sugerencia de mejora:
# MAGIC
# MAGIC ¿Qué hace el código?
# MAGIC
# MAGIC     Imprime un mensaje: Indica qué ruta está procesando.
# MAGIC     dbutils.fs.rm(..., recurse=True): Borra la carpeta y todo su contenido de forma recursiva.
# MAGIC     dbutils.fs.mkdirs(...): Vuelve a crear la carpeta vacía inmediatamente.
# MAGIC     Finaliza el mensaje: Imprime "Done" en la misma línea.
# MAGIC
# MAGIC ######     def clean_ingest_data(self, source, dest):
# MAGIC
# MAGIC Este código descarga archivos de un repositorio de GitHub en paralelo utilizando hilos. Aquí tienes algunos puntos clave para mejorarlo o tener en cuenta:
# MAGIC
# MAGIC     - Manejo de Carpetas: El API de GitHub devuelve una lista de objetos. Si source contiene subcarpetas, el código fallará al intentar obtener 
# MAGIC       download_url (que es None para directorios) o al intentar escribir en una ruta local que no existe.
# MAGIC     - Límite de API: GitHub tiene límites de tasa (rate limits). Si descargas muchos archivos, podrías recibir un error 403. Es recomendable pasar un Token 
# MAGIC       de Acceso en los headers.
# MAGIC     - Seguridad de Rutas: Asegúrate de que dest_path exista antes de escribir. Aunque llamas a cleanup_dir, si esa función solo borra contenido, el open() 
# MAGIC       fallará si la carpeta raíz no está creada.
# MAGIC     - Eficiencia con deque: Usar collections.deque sobre el map del executor es un truco ingenioso para forzar la ejecución de las funciones sin un 
# MAGIC       bucle explícito, pero puede ser menos legible que un simple for _ in executors.map(...).
# MAGIC
# MAGIC ![image_1775081576326.png](./image_1775081576326.png "image_1775081576326.png")
# MAGIC
# MAGIC https://www.google.com/search?q=import+requests++++++++%0D%0A++++++++from+concurrent.futures+import+ThreadPoolExecutor%0D%0A++++++++import+collections%0D%0A%0D%0A++++++++dest_path+%3D+f%22%2FVolumes%2F%7Bself.catalog%7D%2F%7Bself.db%7D%2F%7Bself.volume_name%7D%2F%7Bdest%7D%22%0D%0A++++++++self.cleanup_dir%28dest_path%29%0D%0A%0D%0A++++++++api_url+%3D+f%22https%3A%2F%2Fapi.github.com%2Frepos%2F%7Bself.owner%7D%2F%7Bself.repo%7D%2Fcontents%2F%7Bsource%7D%22%0D%0A++++++++files+%3D+requests.get%28api_url%29.json%28%29%0D%0A++++++++download_urls+%3D+%5Bfile%5B%22download_url%22%5D+for+file+in+files%5D%0D%0A%0D%0A++++++++def+download_files%28download_url%29%3A%0D%0A++++++++++++filename+%3D+download_url.split%28%22%2F%22%29%5B-1%5D%0D%0A++++++++++++with+requests.get%28download_url%2C+stream%3DTrue%29+as+r%3A++++++++++++++++%0D%0A++++++++++++++++with+open%28f%22%7Bdest_path%7D%2F%7Bfilename%7D%22%2C+%22wb%22%29+as+f%3A%0D%0A++++++++++++++++++++for+chunk+in+r.iter_content%28chunk_size%3D8192%29%3A%0D%0A++++++++++++++++++++++++f.write%28chunk%29+++++++++++++++%0D%0A%0D%0A++++++++print%28f%22Downloading+files...%22%2C+end%3D%27%27%29%0D%0A++++++++with+ThreadPoolExecutor%28max_workers%3D2%29+as+executors%3A%0D%0A++++++++++++collections.deque%28executors.map%28download_files%2C+download_urls%29%29%0D%0A++++++++print%28%22Done%22%29&client=firefox-b-d&hs=mUI&sca_esv=b78cf8500232fcdc&sxsrf=ANbL-n4MuRBhqsoux4mdvF3iVldi7dO7Vg%3A1775081255316&udm=50&fbs=ADc_l-bpk8W4E-qsVlOvbGJcDwpn60DczFdcvPnuv8WQohHLTcTH442KskDyC74HpRHrYUquKPmtPp4IEQPDuCAuszg_f6lrMfx1yzeTgwnfyamc0JIMf2V3Z6mCBvl4xpDZGgkvKXJqd3fw5XwOaa-eRvSwzTIJ8lMPDTTHYnugv3np2z5bPG3UpjP_3mUObM7nSUmPvI_1CQnzO9bihVeWxCjAdOOcoXMAQ2_iaMZRlj1xkoWNibM&aep=1&ntc=1&sa=X&ved=2ahUKEwigsK791M2TAxWtIUQIHeRIHhAQ2J8OegQIDxAE&biw=1680&bih=739&dpr=1.13&mstk=AUtExfCjz-RGUdhPjAbCcDuYZXy930BS4dcLBiut0n7vFrc5c5JAe3pyTVOGfN49SIyIvUDYsh-KR6Wk5bvbUkMqwa74LLwxW9YRZJf_fBiXXDMQ772fw8fgnSf3QL4VUEOl958FZ4ZtrSR_szuZY2jZh0nVQcmLx0T6MpC-HEa3ji20RkKc4LSwE0LYthX7GcnDbNvii52AMQEwMa0auuEnjbI9OFY8lB4WaPnmiyQq-y189K500dlJhWdbAYZJZbStFpjZov7z14Ny8wbt1XIIW57AF-0wOyUZgMNzge4wWpx6bJPGRXUSJivdSXP71hJeD1rLsmd9IttucA&csuir=1&mtid=KZfNabWMHrKsmtkPj7qhgAw
# MAGIC
# MAGIC
# MAGIC ##### concurrent.futures import ThreadPoolExecutor
# MAGIC
# MAGIC ![image_1775150985264.png](./image_1775150985264.png "image_1775150985264.png")
# MAGIC
# MAGIC ![image_1775151054934.png](./image_1775151054934.png "image_1775151054934.png")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Requests library in Python, 
# MAGIC To use it you must first install it using a package manager like pip, as it is not part of the standard library
# MAGIC ###### Common Methods: 
# MAGIC Supports all standard HTTP methods including .get(), .post(), .put(), .delete(), and .patch().
# MAGIC
# MAGIC ![image_1775152232797.png](./image_1775152232797.png "image_1775152232797.png")
# MAGIC

# COMMAND ----------

