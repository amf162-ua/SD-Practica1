import sys
import subprocess
import os
import tkinter as tk
from tkinter import ttk, messagebox

class WSLauncher:
    def __init__(self, root):
        self.root = root
        self.root.title("WaterManagement — Launcher de Estaciones (PC LAB 2)")
        self.root.geometry("550x400")
        self.procesos = []

        self._crear_interfaz()

    def _crear_interfaz(self):
        header = tk.Label(
            self.root, 
            text="LANZADOR DE ESTACIONES DE RIEGO (WS_M + WS_E)", 
            font=("Helvetica", 10, "bold"), 
            bg="#27ae60", 
            fg="white", 
            pady=8
        )
        header.pack(fill=tk.X)

        # Configuración de Red (Railway / Cloud)
        frame_net = ttk.LabelFrame(self.root, text=" Conexión a Central & Kafka (Railway/Cloud) ", padding=10)
        frame_net.pack(fill=tk.X, padx=10, pady=5)

        ttk.Label(frame_net, text="IP/Host Central:").grid(row=0, column=0, padx=5, pady=2, sticky=tk.W)
        self.entry_ip_c = ttk.Entry(frame_net, width=20)
        self.entry_ip_c.insert(0, "127.0.0.1")  # O la URL/IP de Railway
        self.entry_ip_c.grid(row=0, column=1, padx=5, pady=2)

        ttk.Label(frame_net, text="Puerto Sockets Central:").grid(row=0, column=2, padx=5, pady=2, sticky=tk.W)
        self.entry_puerto_c = ttk.Entry(frame_net, width=8)
        self.entry_puerto_c.insert(0, "5000")
        self.entry_puerto_c.grid(row=0, column=3, padx=5, pady=2)

        ttk.Label(frame_net, text="IP/Host Kafka:").grid(row=1, column=0, padx=5, pady=2, sticky=tk.W)
        self.entry_ip_k = ttk.Entry(frame_net, width=20)
        self.entry_ip_k.insert(0, "127.0.0.1")
        self.entry_ip_k.grid(row=1, column=1, padx=5, pady=2)

        ttk.Label(frame_net, text="Puerto Kafka:").grid(row=1, column=2, padx=5, pady=2, sticky=tk.W)
        self.entry_puerto_k = ttk.Entry(frame_net, width=8)
        self.entry_puerto_k.insert(0, "9092")
        self.entry_puerto_k.grid(row=1, column=3, padx=5, pady=2)

        # Lanzador de Estación
        frame_ws = ttk.LabelFrame(self.root, text=" Crear Estación de Riego ", padding=10)
        frame_ws.pack(fill=tk.X, padx=10, pady=5)

        ttk.Label(frame_ws, text="ID Estación:").grid(row=0, column=0, padx=5, pady=2)
        self.entry_ws_id = ttk.Entry(frame_ws, width=10)
        self.entry_ws_id.insert(0, "WS_01")
        self.entry_ws_id.grid(row=0, column=1, padx=5, pady=2)

        ttk.Label(frame_ws, text="Puerto Local Engine:").grid(row=0, column=2, padx=5, pady=2)
        self.entry_puerto_ws = ttk.Entry(frame_ws, width=8)
        self.entry_puerto_ws.insert(0, "6001")
        self.entry_puerto_ws.grid(row=0, column=3, padx=5, pady=2)

        btn_crear = ttk.Button(frame_ws, text="▶ Lanzar WS (Monitor + Engine)", command=self.lanzar_estacion)
        btn_crear.grid(row=1, column=0, columnspan=4, pady=10)

        # Cierre
        frame_ctrl = ttk.Frame(self.root, padding=10)
        frame_ctrl.pack(fill=tk.X, padx=10, pady=5)

        self.lbl_estado = ttk.Label(frame_ctrl, text="Estaciones activas: 0", font=("Helvetica", 9, "bold"))
        self.lbl_estado.pack(side=tk.LEFT)

        btn_stop = ttk.Button(frame_ctrl, text="🛑 Detener Estaciones", command=self.detener)
        btn_stop.pack(side=tk.RIGHT)

    def lanzar_estacion(self):
        ip_c = self.entry_ip_c.get().strip()
        puerto_c = self.entry_puerto_c.get().strip()
        ip_k = self.entry_ip_k.get().strip()
        puerto_k = self.entry_puerto_k.get().strip()
        ws_id = self.entry_ws_id.get().strip()
        puerto_ws = self.entry_puerto_ws.get().strip()

        if not ws_id or not puerto_ws:
            messagebox.showwarning("Error", "Faltan datos de la estación.")
            return

        ws_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "WM_WS")
        script_m = os.path.join(ws_dir, "WM_WS_M.py")
        script_e = os.path.join(ws_dir, "WM_WS_E.py")

        # 1. Abrir Monitor en consola propia
        cmd_m = [sys.executable, script_m, puerto_ws, ip_c, puerto_c, ws_id]
        if sys.platform == "win32":
            p_m = subprocess.Popen(["cmd.exe", "/c", "start", f"Monitor_{ws_id}"] + cmd_m, cwd=ws_dir)
        else:
            p_m = subprocess.Popen(["x-terminal-emulator", "-e"] + cmd_m, cwd=ws_dir)
        self.procesos.append(p_m)

        # 2. Abrir Engine en consola propia
        cmd_e = [sys.executable, script_e, ip_k, puerto_k, "127.0.0.1", puerto_ws, ws_id]
        if sys.platform == "win32":
            p_e = subprocess.Popen(["cmd.exe", "/c", "start", f"Engine_{ws_id}"] + cmd_e, cwd=ws_dir)
        else:
            p_e = subprocess.Popen(["x-terminal-emulator", "-e"] + cmd_e, cwd=ws_dir)
        self.procesos.append(p_e)

        # Auto-incrementar para la siguiente estación
        self.entry_puerto_ws.delete(0, tk.END)
        self.entry_puerto_ws.insert(0, str(int(puerto_ws) + 1))
        
        num = "".join(filter(str.isdigit, ws_id))
        if num:
            self.entry_ws_id.delete(0, tk.END)
            self.entry_ws_id.insert(0, f"WS_{int(num)+1:02d}")

        self.lbl_estado.config(text=f"Procesos de estación lanzados: {len(self.procesos)//2}")

    def detener(self):
        for p in self.procesos:
            try:
                p.terminate()
            except Exception:
                pass
        self.procesos.clear()
        self.lbl_estado.config(text="Estaciones activas: 0")

if __name__ == "__main__":
    root = tk.Tk()
    app = WSLauncher(root)
    root.mainloop()