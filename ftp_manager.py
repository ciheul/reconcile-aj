import ftplib
from local import config
import socket
from datetime import datetime


class FtpManager:

    _ftp = None

    FAIL = -1
    SUCCESS = 0

    def connect(self):
        # TODO add error handling
        try:
            self._ftp = ftplib.FTP(config.SERVER)
            self._ftp.login(config.USER, config.PASS)
            return self.SUCCESS
        except socket.error:
            return self.FAIL

    def disconnect(self):
        self._ftp.quit()

    def upload_ftr(self, filepath, path=config.FTR_DIR):
        try:
            self._ftp.cwd(path)
            if '/' in filepath:
                filename = filepath.split('/')[-1:].pop()
            else:
                filename = filepath
            self._ftp.storlines("STOR %s" % filename, open(filepath, 'rb'))
            return self.SUCCESS
        except: 
            return self.FAIL

    def download_fcn(self, filename, path=config.FCN_DIR):
        try:
            #raise ftplib.error_reply
            self._ftp.cwd(path)
            self._ftp.retrbinary("RETR %s" % filename,
                                 open('fcn/%s' % filename, 'wb').write)
            return self.SUCCESS
        except:
            return self.FAIL

    def show_list_ftr(self, path=config.FTR_DIR):
        self._ftp.cwd(path)
        self._ftp.retrlines('LIST')

    def delete_ftr(self, filename, path=config.FTR_DIR):
        self._ftp.cwd(path)
        self._ftp.delete(filename)
