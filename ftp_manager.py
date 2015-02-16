from ftplib import FTP
import config


class FtpManager:

    _ftp = None

    def connect(self):
        # TODO add error handling
        self._ftp = FTP(config.HOST)
        self._ftp.login(config.USER, config.PASS)

    def disconnect(self):
        self._ftp.quit()

    def upload_ftr(self, filepath, path=config.FTR_DIR):
        self._ftp.cwd(path)
        if '/' in filepath:
            filename = filepath.split('/')[-1:].pop()
        else:
            filename = filepath
        self._ftp.storlines("STOR %s" % filename, open(filepath, 'rb'))

    def show_list_ftr(self, path=config.FTR_DIR):
        self._ftp.cwd(path)
        self._ftp.retrlines('LIST')

    def delete_ftr(self, filename, path=config.FTR_DIR):
        self._ftp.cwd(path)
        self._ftp.delete(filename)
