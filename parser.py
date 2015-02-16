import os
import os.path
import simplejson as json
import sys

sys.path.append(os.path.join('.', 'gen-py'))

from paymentpoint import MessageParser as Parser
from redis import Redis
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
from thrift.server import TServer


r = Redis()

class ParserImpl(Parser.Iface):
    def parse(self, billercode, productcode, billnumber, bit61):
        res = self.parse_bit61(billercode, productcode, billnumber, bit61)
        res = res['structured']
        result = " | ".join(" : ".join(map(str,l[:2])) for l in res)
        return result

    def parse_prepaid(self, billercode, productcode, billnumber, bit61, bit48):
        res = self.parse_bit61_prepaid(billercode, productcode,
                                       billnumber, bit61, bit48)
        res = res['structured']
        result = " | ".join(" : ".join(map(str,l)) for l in res)
        return result

    def ctdts(self, number):
        en_number = '{:,}'.format(number)
        return en_number.replace('.', '%temp%').replace(',', '.'). \
                replace('%temp%', ',')

    def parse_bit61(self, biller_code, product_code, bill_number, bit61):
        try:
            key = 'billformat:%s:%s' % (biller_code, product_code)

            # get message format
            bf = json.loads(r.get(key))
            if 'pointer' in bf:
                bf = json.loads(r.get(bf['pointer']))

            # {'structured: [('field', 'value')]'}
            result = dict()

            # reprint:
            # field1: value1
            # field2: value2
            # [('field', 'value'), ...]
            structured = list()

            # {'field': value, ...}
            unstructured = dict()

            if bf['format_type'] == 1:
                position = 0

                repeat_number = 0
                repeat_rows = list()
                # <rows>
                for row in bf['formats']:
                    # row that has a repeat field is collected first and will be
                    # processed later
                    if 'repeat' in row:
                        repeat_rows.append(row)
                        continue

                    if 'dynamic' in row:
                        # dynamic=1 is just for finnet jastel and kartuhalo
                        if int(row['dynamic']) == 1:
                            start = 20 + 23 * int(bit61[19:20])
                            name = bit61[start:start+30]
                            structured.append((row['field'], name))
                        continue

                    # parse part of message and cleanse
                    v = bit61[position:position+int(row['length'])]
                    v = v.replace('.', '').strip()

                    #print row['field'], v

                    # get all fields to unstructured dictionary
                    unstructured[row['field']] = v

                    # get the number of repetition for repeated rows
                    if 'repeat_number' in row:
                        repeat_number = int(v)

                    # if row will show up, otherwise unnecessary
                    if int(row['show']) == True:
                        # if value is money, print it using currency
                        if 'type' in row:
                            if row['type'] == 'money':
                                structured.append((row['field'], self.ctdts(int(v)),
                                                  row['type']))
                            elif row['type'] == 'bill_number':
                                structured.append((row['field'], bill_number))
                        else:
                            structured.append((row['field'], v))
                    position += int(row['length'])

                # check whether there are repeated rows
                if 'repeat_row' in bf and bf['repeat_row'] == 1:
                    repeat_ascending = int(bf['repeat_ascending'])

                    # prepare range() in ascending or descending order
                    if repeat_ascending == 1:
                        num_range = range(1, repeat_number+1)
                    else:
                        num_range = range(repeat_number, 0, -1)

                    # process repeated rows
                    for i in num_range:
                        for repeat_row in repeat_rows:
                            # parse part of message and cleanse
                            v = bit61[
                                position:position+int(repeat_row['length'])]
                            v = v.replace('.', '').strip()

                            #unstructured[repeat_row['field']] = v
                            unstructured.setdefault(repeat_row['field'], []).append(v)

                            if 'type' in repeat_row:
                                if repeat_row['type'] == 'money':
                                    new_field = \
                                        repeat_row['field'] + ' #' + str(i)
                                    structured.append((new_field, self.ctdts(int(v)),
                                                      repeat_row['type']))
                            else:
                                new_field = repeat_row['field'] + ' #' + str(i)
                                structured.append((new_field, v))
                            position += int(repeat_row['length'])

            elif bf['format_type'] == 2:
                for row in bf['formats']:
                    # parse part of message and cleanse
                    v = bit61[
                        int(row['start']):int(row['start'])+int(row['length'])]
                    v = v.replace('.', '').strip()

                    # hidden field is ignored
                    if int(row['show']) == True:

                        # if value is money, print it using currency
                        if 'type' in row:
                            structured.append((row['field'], self.ctdts(int(v)),
                                              row['type']))
                        else:
                            structured.append((row['field'], v))

                    # extracted value is returned
                    if 'return' in row:
                        result[row['return']] = v

            result['structured'] = structured
            result['unstructured'] = unstructured
            return result
        except Exception, e:
            #print e
            return Exception


    def parse_bit61_prepaid(self, biller_code, product_code, bill_number,
                            bit61, bit48):
        structured = list()
        result = dict()

        no_voucher = bit48.split(';')[2].split(':')[1]
        splitted = bit61.split()
        structured = [
            ('No. Pelanggan', splitted[0]),
            ('No. Voucher', no_voucher),
            ('Nominal', self.ctdts(int(splitted[1])), 'money'),
        ]
        result['structured'] = structured
        return result


if __name__ == '__main__':
    processor = Parser.Processor(ParserImpl())
    transport = TSocket.TServerSocket(port=18181)
    tbfactory = TTransport.TBufferedTransportFactory()
    pbfactory = TBinaryProtocol.TBinaryProtocolFactory()

    server = TServer.TThreadedServer(processor, transport, tbfactory,
                                     pbfactory)
    print 'Starting the Parser Server..'
    server.serve()
