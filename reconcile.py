from local import config

from django.conf import settings

settings.configure(
    DATABASES = {
        'default': {
            'ENGINE': 'django.db.backends.postgresql_psycopg2',
            'NAME': config.NAME,
            'USER': config.USERNAME,
            'PASSWORD': config.PASSWORD,
            'HOST': config.HOST,
            'PORT': config.PORT,
        }
    }
)
    

import os
import os.path
import sys
sys.path.append(os.path.join('.', 'gen-py'))

from adm.models import Transaction
import parser
from ftp_manager import FtpManager

from datetime import datetime, time, timedelta
import logging
from logging.handlers import TimedRotatingFileHandler
import shutil

from redis import Redis
from rq_scheduler import Scheduler


# python-RQ Scheduler
scheduler = Scheduler(connection=Redis())

# LOGGER
LOG_FOLDER = '../log/reconcile'
if not os.path.exists(LOG_FOLDER):
    os.mkdir(LOG_FOLDER)

LOG_NAME = os.path.join(LOG_FOLDER, 'reconcile.log')
LOG_FORMAT = "%(asctime)s %(levelname)s - %(message)s"

logger = logging.getLogger("Rotating Log")
logger.setLevel(logging.INFO)

handler = TimedRotatingFileHandler(LOG_NAME, when="midnight")

formatter = logging.Formatter(fmt=LOG_FORMAT, datefmt="%Y-%m-%d %H:%M:%S")
handler.setFormatter(formatter)
logger.addHandler(handler)

#logging.basicConfig(filename=LOG_NAME, format=LOG_FORMAT,
#                    level=logging.INFO, datefmt="%Y-%m-%d %I:%M:%S")

class Reconcile:
    header_postpaid = 'DT|SWITCHERID|MERCHANT|REFNUM|SREFNUM|IDPEL|BL_TH|TRAN_AMOUNT|RP_TAG|RP_INSENTIF|VAT|RP_BK|BANKCODE'
    header_prepaid = 'DT|SWITCHERID|MERCHANT|REFNUM|SREFNUM|METERNUM|TRAN_AMOUNT|ADMIN|RP_STAMPDUTY|RP_VAT|RP_PLT|RP_CPI|PP|PU|TOKEN|BANKCODE'
    header_nontaglis = 'DT|SWITCHERID|MERCHANT|REFNUM|SREFNUM|IDPEL|REGNUM|DT_REGISTRATION|TRAN_CODE|TRAN_AMOUNT|BANKCODE'
    
    SWITCHER_ID = 'AJ100A3'
    MERCHANT = '6021'
    BANK_CODE = '000735'

    RECONCILE_1 = 1
    RECONCILE_2 = 2 
    RECONCILE_3 = 3

    FRI = 4
    SAT = 5
    SUN = 6

    FTR_QUEUE = 'queue'
    FTR_LOCAL = 'ftr'
    FCN_LOCAL = 'fcn'


    def __init__(self):
        """Initiate to access parser service."""
        self.parser = parser.ParserImpl()

        self.ftp = FtpManager()

        # for ftr
        self.ftr_postpaid = None
        self.ftr_prepaid = None
        self.ftr_nontaglis = None

        self.ftrctl_postpaid = None
        self.ftrctl_prepaid = None
        self.ftrctl_nontaglis = None

        self.ftr_ctl_name = None

    def parse_bill_number(self, bill_number):
        """Returns product code and bill_number."""
        if '#' in bill_number:
            split = bill_number.split('#')
            product_code = int(split[0])
            if product_code == 1:
                product_code = 4
            bill_number = split[1]
        else:
            if len(bill_number) == 11: product_code = 2   # prepaid
            elif len(bill_number) == 12: product_code = 4 # postpaid
            elif len(bill_number) == 13: product_code = 3 # nontaglis
            else: return None
        return product_code, bill_number

    def add_zero_padding(self, n, length):
        """Return a number with zero left padding."""
        zero_padded = length - len(str(n))
        return zero_padded * '0' + str(n)

    def determine_reconcile_type(self):
        """ Return reconcile type.
            1 == Monday, Tuesday, Wednesday, Thursday
            2 == Friday, Saturday, Sunday
            3 == Holiday
        """
        now = datetime.now()

        if now.weekday() in [self.FRI, self.SAT, self.SUN]:
            return self.RECONCILE_2

        if now.date() in self.get_holidays():
            return self.RECONCILE_3

        return self.RECONCILE_1

    def get_holidays(self):
        """Return a list of holidays for current year."""
        holidays = list()

        filename = 'holiday-%s.txt' % datetime.now().year
        file_path = os.path.join('holiday', filename)

        with open(file_path) as f:
            for line in f.readlines():
                if line is None: continue
                if line == "\n": continue
                if line.startswith('#'): continue

                line = line.strip()
                if '#' in line:
                    spl = line.split('#')
                    line = spl[0].strip() 

                holiday = datetime.strptime(line, '%Y-%m-%d').date()
                holidays.append(holiday)
        return holidays

    def generate_ftr_ctl(self):
        print "GENERATE_FTR_CTL"
        """Generate ftr and ftr.ctl and store to list."""
        # for ftr
        self.ftr_postpaid = list()
        self.ftr_prepaid = list()
        self.ftr_nontaglis = list()

        # for ftr
        self.ftr_postpaid.append(self.header_postpaid)
        self.ftr_prepaid.append(self.header_prepaid)
        self.ftr_nontaglis.append(self.header_nontaglis)

        post_aggregate = {
            'counter': 0,
            'amount': 0,
            'total': 0,
            'ins_dis': 0,
            'vat': 0,
            'penalty': 0,
        }

        pre_aggregate = {
            'counter': 0,
            'amount': 0,
            'admin_charge': 0,
            'stamp_duty': 0,
            'vat': 0,
            'plt': 0,
            'cpi': 0,
            'power_purchase': 0,
            'purchased_kwh': 0,
        }

        ntl_aggregate = {
            'counter': 0,
            'amount': 0,
        }

        # TODO filter based on date
        # this loop filters line for postpaid, prepaid, and nontaglis in a loop
        # for the performance purpose.
        try:
            today = datetime.now().date()
            yesterday = today - timedelta(days=1)

            for i in Transaction.objects.filter(
                    product__internal_code__contains='PLN') \
                        .order_by('timestamp'):
                # status fail or pending is ignored
                if i.status != 3: continue

                product_code, bill_number = \
                    self.parse_bill_number(i.bill_number)

                result = self.parser.parse_bit61(i.product.biller.code,
                                                 product_code,
                                                 bill_number,
                                                 i.bit_48)

                if result is Exception: continue

                p = result['unstructured']
                #for j in p: print j, "[" + str(p[j]) + "]"
                #print "==================="

                # generate reconcile line for postpaid 
                if product_code == 4:
                    total_bill = int(p["Jumlah Tagihan Belum Lunas"])
                    if total_bill > 4:
                        total_bill = 4

                    # a bit48 could contain more than one transaction
                    for j in range(total_bill):
                        total_amount = int(p["Tagihan Listrik"][j]) \
                            + int(p["Denda"][j])

                        # add zero left-padding
                        str_total_amount = self.add_zero_padding(total_amount, 
                                                                 12)

                        # aggregate fields
                        post_aggregate['counter'] += 1
                        post_aggregate['amount'] += total_amount
                        post_aggregate['total'] += int(p["Tagihan Listrik"][j])

                        if p["Kode Insentif Disinsentif"][j] == "D":
                            post_aggregate['ins_dis'] += \
                                int(p["Tagihan Listrik"][j])
                        else:
                            post_aggregate['ins_dis'] -= \
                                int(p["Tagihan Listrik"][j])

                        post_aggregate['vat'] += \
                            int(p["Pajak Nilai Tambah"][j])
                        post_aggregate['penalty'] += int(p["Denda"][j])

                        # build rp insentif from 'kode' and 'nilai' 
                        rp_insentif = str()
                        if p["Kode Insentif Disinsentif"][j] == "D":
                            rp_insentif += '+'
                        else:
                            rp_insentif += '-'
                        rp_insentif += p["Nilai Insentif Disinsentif"][j]

                        # construct a line
                        line = "%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s" % (
                            i.timestamp.strftime('%Y%m%d%H%M%S'),
                            self.SWITCHER_ID,
                            self.MERCHANT,
                            p["Kode Referensi PLN"],
                            p["Switcher Receipt Reference Number"],
                            p["Identitas Pelanggan"],
                            p["Periode Tagihan"][j],
                            str_total_amount,
                            p["Tagihan Listrik"][j],
                            rp_insentif,
                            p["Pajak Nilai Tambah"][j],
                            p["Denda"][j],
                            self.BANK_CODE,
                        )

                        # store in a list
                        self.ftr_postpaid.append(line)

                # generate reconcile line for prepaid
                if product_code == 2:
                    if p["Purchased KWH Unit"] == str():
                        p["Purchased KWH Unit"] = 10 * '0'
                    if p["Customer Payables Installment"] == str():
                        p["Customer Payables Installment"] = 10 * '0'
                    if p["Public Lightning Tax"] == str():
                        p["Public Lightning Tax"] = 10 * '0'
                    if p["Stamp Duty"] == str():
                        p["Stamp Duty"] = 10 * '0'
                    if p["Admin Charge"] == str():
                        p["Admin Charge"] = 10 * '0'
                    if p["Value Added Tax"] == str():
                        p["Value Added Tax"] = 10 * '0'
                    if p["Power Purchase"] == str():
                        p["Power Purchase"] = 10 * '0'

                    total_amount = int(p["Admin Charge"]) \
                        + int(p["Stamp Duty"]) \
                        + int(p["Public Lightning Tax"]) \
                        + int(p["Customer Payables Installment"]) \
                        + int(p["Purchased KWH Unit"])

                    str_total_amount = self.add_zero_padding(total_amount, 12)

                    pre_aggregate['counter'] += 1
                    pre_aggregate['amount'] += total_amount
                    pre_aggregate['admin_charge'] += int(p["Admin Charge"])
                    pre_aggregate['stamp_duty'] += int(p["Stamp Duty"])
                    pre_aggregate['vat'] += int(p["Value Added Tax"])
                    pre_aggregate['plt'] += int(p["Public Lightning Tax"])
                    pre_aggregate['cpi'] += \
                        int(p["Customer Payables Installment"])
                    pre_aggregate['power_purchase'] += int(p["Power Purchase"])
                    pre_aggregate['purchased_kwh'] += \
                        int(p["Purchased KWH Unit"])

                    line = "%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s" % (
                        i.timestamp.strftime('%Y%m%d%H%M%S'),
                        self.SWITCHER_ID,
                        self.MERCHANT,
                        p["PLN Reference Number"],
                        p["Switcher Receipt Reference Number"],
                        p["Meter Serial Number"],
                        str_total_amount,
                        p["Admin Charge"],
                        p["Stamp Duty"],
                        p["Value Added Tax"],
                        p["Public Lightning Tax"],
                        p["Customer Payables Installment"],
                        p["Power Purchase"],
                        p["Purchased KWH Unit"],
                        p["Token Number"],
                        self.BANK_CODE,
                    )

                    self.ftr_prepaid.append(line)

                # generate reconcile line for NTL
                if product_code == 3:
                    ntl_aggregate['counter'] += 1
                    ntl_aggregate['amount'] += int(p["Nilai Total Amount"])

                    line = "%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s" % (
                        i.timestamp.strftime('%Y%m%d%H%M%S'),
                        self.SWITCHER_ID,
                        self.MERCHANT,
                        p["Kode Referensi Transaksi"],
                        p["Switcher Receipt Reference Number"],
                        p["ID Pelanggan"],
                        p["Nomor Registrasi"],
                        p["Registration Date"],
                        p["Transaction Code"],
                        p["Nilai Total Amount"],
                        self.BANK_CODE,
                    )

                    self.ftr_nontaglis.append(line)

            logger.info("Query database success.")
        except:
            logger.error("Fail to query database. Schedule task to restart.")
            scheduler.enqueue_in(timedelta(minutes=config.INTERVAL), self.main)
            sys.exit()

        now = datetime.now()
        # aggregate last line for postpaid
        last_line_post = "%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s" % (
            (now.date() + timedelta(days=1)).strftime('%Y%m%d000000'),
            self.SWITCHER_ID,
            '0000',
            self.add_zero_padding(post_aggregate['counter'], 32),
            self.add_zero_padding(0, 32),
            self.add_zero_padding(0, 12),
            self.add_zero_padding(0, 6),
            self.add_zero_padding(post_aggregate['amount'], 12),
            self.add_zero_padding(post_aggregate['total'], 11),
            self.add_zero_padding(post_aggregate['ins_dis'], 12),
            self.add_zero_padding(post_aggregate['vat'], 10),
            self.add_zero_padding(post_aggregate['penalty'], 9),
            self.BANK_CODE,
        )
        self.ftr_postpaid.append(last_line_post)

        self.ftrctl_postpaid = "%s|%s" % (
            self.add_zero_padding(post_aggregate['counter'], 32),
            self.add_zero_padding(post_aggregate['amount'], 12))

        # aggregate last line for prepaid
        last_line_pre = "%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s" % (
            (now.date() + timedelta(days=1)).strftime('%Y%m%d000000'),
            self.SWITCHER_ID,
            '0000',
            self.add_zero_padding(pre_aggregate['counter'], 32),
            self.add_zero_padding(0, 32),
            self.add_zero_padding(0, 12),
            self.add_zero_padding(pre_aggregate['amount'], 12),
            self.add_zero_padding(pre_aggregate['admin_charge'], 10),
            self.add_zero_padding(pre_aggregate['stamp_duty'], 10),
            self.add_zero_padding(pre_aggregate['vat'], 10),
            self.add_zero_padding(pre_aggregate['plt'], 10),
            self.add_zero_padding(pre_aggregate['cpi'], 10),
            self.add_zero_padding(pre_aggregate['power_purchase'], 10),
            self.add_zero_padding(pre_aggregate['purchased_kwh'], 10),
            self.add_zero_padding(0, 20),
            self.BANK_CODE,
        )
        self.ftr_prepaid.append(last_line_pre)

        self.ftrctl_prepaid = "%s|%s" % (
            self.add_zero_padding(pre_aggregate['counter'], 32),
            self.add_zero_padding(pre_aggregate['amount'], 12))

        # aggregate last line for nontaglis
        last_line_ntl = "%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s" % (
            (now.date() + timedelta(days=1)).strftime('%Y%m%d000000'),
            self.SWITCHER_ID,
            '0000',
            self.add_zero_padding(post_aggregate['counter'], 32),
            self.add_zero_padding(0, 32),
            self.add_zero_padding(0, 12),
            self.add_zero_padding(0, 13),
            self.add_zero_padding(0, 8),
            self.add_zero_padding(0, 2),
            self.add_zero_padding(post_aggregate['amount'], 17),
            self.BANK_CODE,
        )
        self.ftr_nontaglis.append(last_line_ntl)

        self.ftrctl_nontaglis = "%s|%s" % (
            self.add_zero_padding(ntl_aggregate['counter'], 32),
            self.add_zero_padding(ntl_aggregate['amount'], 17))

    def dump_ftr_ctl(self):
        """Write from memory to disk in QUEUE folder."""
        if not os.path.exists(self.FTR_QUEUE):
            os.mkdir(self.FTR_QUEUE)

        # create filename for postpaid
        now = datetime.now()
        ftr_post_name = "%s-50501-%s.ftr" % \
            (self.BANK_CODE, now.strftime('%Y%m%d'))

        ftr_pre_name = "%s-50502-%s.ftr" % \
            (self.BANK_CODE, now.strftime('%Y%m%d'))

        ftr_ntl_name = "%s-50504-%s.ftr" % \
            (self.BANK_CODE, now.strftime('%Y%m%d'))

        ftrctl_post_name = "%s-50501-%s.ftr.ctl" % \
            (self.BANK_CODE, now.strftime('%Y%m%d'))

        ftrctl_pre_name = "%s-50502-%s.ftr.ctl" % \
            (self.BANK_CODE, now.strftime('%Y%m%d'))

        ftrctl_ntl_name = "%s-50504-%s.ftr.ctl" % \
            (self.BANK_CODE, now.strftime('%Y%m%d'))

        try:
            with open(os.path.join(self.FTR_QUEUE, ftr_post_name), 'w') as f:
                for line in self.ftr_postpaid:
                    f.write(line + '\n')
                logger.info("Dump %s" % \
                    os.path.join(self.FTR_QUEUE, ftr_post_name))

            with open(os.path.join(self.FTR_QUEUE, ftr_pre_name), 'w') as f:
                for line in self.ftr_prepaid:
                    f.write(line + '\n')
                logger.info("Dump %s" % \
                    os.path.join(self.FTR_QUEUE, ftr_pre_name))

            with open(os.path.join(self.FTR_QUEUE, ftr_ntl_name), 'w') as f:
                for line in self.ftr_nontaglis:
                    f.write(line + '\n')
                logger.info("Dump %s" % \
                    os.path.join(self.FTR_QUEUE, ftr_ntl_name))

            with open(os.path.join(self.FTR_QUEUE, ftrctl_post_name), 'w') as f:
                for line in self.ftrctl_postpaid:
                    f.write(line)
                logger.info("Dump %s" % \
                    os.path.join(self.FTR_QUEUE, ftrctl_post_name))

            with open(os.path.join(self.FTR_QUEUE, ftrctl_pre_name), 'w') as f:
                for line in self.ftrctl_prepaid:
                    f.write(line)
                logger.info("Dump %s" % \
                    os.path.join(self.FTR_QUEUE, ftrctl_pre_name))

            with open(os.path.join(self.FTR_QUEUE, ftrctl_ntl_name), 'w') as f:
                for line in self.ftrctl_nontaglis:
                    f.write(line)
                logger.info("Dump %s" % \
                    os.path.join(self.FTR_QUEUE, ftrctl_ntl_name))

            # push to buffer
            self.ftr_ctl_name = list()
            self.ftr_ctl_name.append(ftr_post_name)
            self.ftr_ctl_name.append(ftr_pre_name)
            self.ftr_ctl_name.append(ftr_ntl_name)
            self.ftr_ctl_name.append(ftrctl_post_name)
            self.ftr_ctl_name.append(ftrctl_pre_name)
            self.ftr_ctl_name.append(ftrctl_ntl_name)
            logger.info("Dump success.")
        except IOError:
            scheduler.enqueue_in(timedelta(minutes=config.INTERVAL), self.main)
            logger.error("Dumping to disk fails. Schedule task to restart.")
            sys.exit()

    def upload(self):
        """Upload FTR and FTR.CTL file."""
        if not os.path.exists(self.FTR_QUEUE):
            os.mkdir(self.FTR_QUEUE)

        if not os.path.exists(self.FTR_LOCAL):
            os.mkdir(self.FTR_LOCAL)

        #job_id = scheduler.enqueue_in(timedelta(minutes=1), self.upload)
        #print job_id

        status = self.ftp.connect()

        if status == FtpManager.SUCCESS:
            # upload any files remaining in the queue,
            # either it is new or from previous days
            for i in os.listdir(self.FTR_QUEUE):
                src = os.path.join(self.FTR_QUEUE, i)
                dst = os.path.join(self.FTR_LOCAL, i)

                # upload to server
                status = self.ftp.upload_ftr(src)
                if status == FtpManager.FAIL:
                    break
                logger.info("Upload %s" % src)

                # move uploaded file from queue folder to ftr folder
                self.move(src, dst)

            self.ftp.disconnect()
        else:
            logger.error("Fail to connect FTP server.")

        # when fail occurs, schedule next upload task in a few minutes
        # NOTE this lines MUST be outside from self.ftp.connect and disconnect
        # otherwise, scheduled task will output error.
        if status == FtpManager.FAIL:
            scheduler.enqueue_in(timedelta(minutes=1), self.main)
            logger.error("Upload fail. Schedule task to restart.")
        else:
            logger.info("Upload success.")
 
    def download(self):
        """Download FCN file."""
        if not os.path.exists(self.FCN_LOCAL):
            os.mkdir(self.FCN_LOCAL)

        status = self.ftp.connect()

        if status == FtpManager.SUCCESS:
            # get current date
            now = datetime.now().strftime('%Y%m%d')

            # download the fcn and fcn.ctl
            for i in [1, 2, 4]:
                for ext in ['ftr', 'ftr.ctl']:
                    filename = '000735-5050%d-%s.%s' % (i, now, ext)
                    logger.info("Download %s" % filename)
                    status = self.ftp.download_fcn(filename)
                    if status == FtpManager.FAIL:
                        break
            self.ftp.disconnect()
        else:
            logger.error("Fail to connect FTP server.")

        if status == FtpManager.FAIL:
            scheduler.enqueue_in(timedelta(minutes=config.INTERVAL), self.main)
            logger.error("Download fail. Schedule task to restart.")
        else:
            logger.info("Download success.")

    def move(self, src, dst):
        """Move an uploaded file from queue to ftr."""
        shutil.move(src, dst)
        logger.debug("Move %s to %s" % (src, dst))

    def main(self):
        # create a log folder for reconcile
        if not os.path.exists(LOG_FOLDER):
            os.mkdir(LOG_FOLDER)
        
        #self.generate_ftr_ctl()
        #self.dump_ftr_ctl()
        #self.upload()

        self.download()

        now = datetime.now()

        # step 2. CA upload ftr and ftr.ctl
        if time(0, 0, 0) <= now.time() <= time(8, 30, 0):
             self.generate_ftr_ctl()
             self.dump_ftr_ctl()

             if now.today() not in self.get_holidays() \
                     or now.today() not in [self.FRI, self.SAT, self.SUN]:
                 for i in get_all_buffer():
                     self.upload()
        # step 5. CA downloads fcn
        elif time(12, 0, 0) <= now.time() <= time(13, 0, 0):
             self.download()


if __name__ == '__main__':
    reconcile = Reconcile()
    reconcile.main()
