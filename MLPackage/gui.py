import tkinter
import sys
from tkinter import messagebox, StringVar
from os import startfile
from multiprocessing.pool import ThreadPool
from tkinter import filedialog
import time



class gui_test():

    def __init__(self):
        self.reboot_pool = ThreadPool()
        pass

    def run(self):
        self.__layout__()
        pass

    def getrun(self):

        messagebox.showinfo("Title")


    #label = tkinter.Label(window, width=30)
    #label.pack(padx=20, pady=20)
    #self.getrun(10)




    def getFrame(self):
        pass

    def getclose(self):
        sys.exit()
        pass

    def getLogs(self):
        startfile("C:\\Users\\rishabh bhardwaj\\PycharmProjects\\Selenium\\Unit_Test\\qwertyui.log")

    def getLoc(self):
        startfile("C:\\Users\\rishabh bhardwaj\\Desktop\\snap")
        pass

    def getExcel(self):
        self.import_file_path = filedialog.askopenfilename()
        pass

    def sel(self):
        self.selection = str(self.var.get())

    def asyncCall(self):
        self.reboot_pool.apply_async(self.getrun)

    def __layout__(self):

        window = tkinter.Tk()
        window.title("TESTING TOOL")

        label = tkinter.Label(window, text="URL", width=2, height=4, padx=2)
        label.grid(row=1, column=0)

        self.textbox = tkinter.Text(window, height=1.4, width=40, padx=2)
        self.textbox.grid(row=1, column=1)

        Bt1 = tkinter.Button(text="Run Test", fg="Green", width=6, height=2, padx=20, command=self.asyncCall)
        Bt1.grid(row=5, column=0)

        Bt2 = tkinter.Button(text="Abort", fg="red", width=6, height=2, padx=20, command=self.getclose)
        Bt2.grid(row=5, column=4)

        Bt3 = tkinter.Button(text="Screenshot", fg="Green", command=self.getLoc, width=6, height=2, padx=20)
        Bt3.grid(row=5, column=2, sticky='e')  # .place(relx=0.1, rely=0.2, anchor="e")

        Bt4 = tkinter.Button(text="logs", fg="Green", command=self.getLogs, width=6, height=2, padx=20)
        Bt4.grid(row=5, column=3)

        browseButton_Excel = tkinter.Button(text='Import Excel File', command=self.getExcel, bg='green', fg='white',
                                            font=('helvetica', 12, 'bold'))
        browseButton_Excel.grid(row=1, column=3)

        self.var = StringVar(window, "1")
        self.selection = "1"
        R1 = tkinter.Radiobutton(window, text="Screenshot Yes", variable=self.var, value=1, command=self.sel)
        R1.grid(row=2, column=0)

        R2 = tkinter.Radiobutton(window, text="Screenshot No", variable=self.var, value=2, command=self.sel)
        R2.grid(row=3, column=0)

        col_count, row_count = window.grid_size()

        for col in range(col_count + 1):
            window.grid_columnconfigure(col, minsize=5, weight=1, pad=5)
            pass

        for row in range(row_count + 2):
            window.grid_rowconfigure(row, minsize=20)
            pass

        # Disable resize
        window.resizable(0, 0)
        window.mainloop()
        return

    pass


test = gui_test()
test.__layout__()
