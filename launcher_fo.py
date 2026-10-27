import sys
import subprocess
import os
import tkinter as tk
from tkinter import ttk

class FOLauncher:
    def __init__(self, root):
        self.root = root
        self.root.title("WaterManagement — Launcher de Operarios (PC LAB 1)")
        self.root.geometry("500x350")
        self.procesos = []

        self._crear_interfaz()

    def _crear_interfaz(self):
        header = tk.Label(
            self.root, 
            text="LANZADOR DE OPERARIOS DE CAMPO (WM_FO)", 
            font=("Helvetica", 10, "bold"), 
            bg="#2980b9", 
            fg="white", 
            pady=8
        )
        header.pack(fill=tk.X)

        # Configuración Kafka
        frame_net = ttk.LabelFrame(self.root, text=" Conexión a Kafka (Railway / Cloud) ", padding=10)
        frame_net.pack(fill=tk.X, padx=10, pady=5)

        ttk.Label(frame_net, text="IP/Host Kafka:").grid(row=0, column=0, padx=5, pady=2, sticky=tk.W)
        self.entry_ip_k = ttk.Entry(frame_net, width=18)
        self.entry_ip_k.insert(0, "44.204.33.94")  # Cambiar por IP/Host de Railway
        self.entry_ip_k.grid(row=0, column=1, padx=5, pady=2)

        ttk.Label(frame_net, text="Puerto Kafka:").grid(row=0, column=2, padx=5, pady=2, sticky=tk.W)
        self.entry_puerto_k = ttk.Entry(frame_net, width=8)
        self.entry_puerto_k.insert(0, "9092")
        self.entry_puerto_k.grid(row=0, column=3, padx=5, pady=2)

        # Crear Operario
        frame_fo = ttk.LabelFrame(self.root, text=" Crear Operario de Campo ", padding=10)
        frame_fo.pack(fill=tk.X, padx=10, pady=5)

        ttk.Label(frame_fo, text="ID Operario:").grid(row=0, column=0, padx=5, pady=2)
        self.entry_op_id = ttk.Entry(frame_fo, width=12)
        self.entry_op_id.insert(0, "OP_01")
        self.entry_op_id.grid(row=0, column=1, padx=5, pady=2)

        ttk.Label(frame_fo, text="Fichero (Opcional):").grid(row=0, column=2, padx=5, pady=2)
        self.entry_fichero = ttk.Entry(frame_fo, width=15)
        # Dejado en blanco por defecto para arrancar en MODO INTERACTIVO
        self.entry_fichero.grid(row=0, column=3, padx=5, pady=2)

        btn_crear = ttk.Button(frame_fo, text="▶ Lanzar Operario (Modo Interactivo)", command=self.lanzar_operario)
        btn_crear.grid(row=1, column=0, columnspan=4, pady=10)

        # Control de Cierre
        frame_ctrl = ttk.Frame(self.root, padding=10)
        frame_ctrl.pack(fill=tk.X, padx=10, pady=5)

        self.lbl_estado = ttk.Label(frame_ctrl, text="Operarios activos: 0", font=("Helvetica", 9, "bold"))
        self.lbl_estado.pack(side=tk.LEFT)

        btn_stop = ttk.Button(frame_ctrl, text="🛑 Detener Operarios", command=self.detener)
        btn_stop.pack(side=tk.RIGHT)

    def lanzar_operario(self):
        ip_k = self.entry_ip_k.get().strip()
        puerto_k = self.entry_puerto_k.get().strip()
        op_id = self.entry_op_id.get().strip()
        fichero = self.entry_fichero.get().strip()

        fo_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "WM_FO")
        script_fo = os.path.join(fo_dir, "WM_FO.py")

        cmd = [sys.executable, "-u", script_fo, ip_k, puerto_k, op_id]
        
        # Solo adjunta el fichero si el usuario lo ha escrito explícitamente y el archivo existe
        if fichero:
            ruta_fichero = fichero if os.path.isabs(fichero) else os.path.join(fo_dir, fichero)
            if os.path.exists(ruta_fichero):
                cmd.append(ruta_fichero)

        if sys.platform == "win32":
            p = subprocess.Popen(["cmd.exe", "/k"] + cmd, creationflags=subprocess.CREATE_NEW_CONSOLE, cwd=fo_dir)
        else:
            p = subprocess.Popen(["x-terminal-emulator", "-e"] + cmd, cwd=fo_dir)

        self.procesos.append(p)

        # Auto-incrementar ID para el siguiente operario
        num = "".join(filter(str.isdigit, op_id))
        if num:
            self.entry_op_id.delete(0, tk.END)
            self.entry_op_id.insert(0, f"OP_{int(num)+1:02d}")

        self.lbl_estado.config(text=f"Operarios activos lanzados: {len(self.procesos)}")

    def detener(self):
        for p in self.procesos:
            try:
                p.terminate()
            except Exception:
                pass
        self.procesos.clear()
        self.lbl_estado.config(text="Operarios activos: 0")

if __name__ == "__main__":
    root = tk.Tk()
    app = FOLauncher(root)
    root.mainloop()