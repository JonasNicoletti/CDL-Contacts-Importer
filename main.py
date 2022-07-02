from apscheduler.schedulers.blocking import BlockingScheduler
from dataclasses import dataclass
from typing import List, Dict
from datetime import datetime
import json
from collections import defaultdict
import pprint


@dataclass
class Contact:

    nome: str
    cognome: str
    email: str
    telefono: str
    regione: str
    provincia: str
    citta: str
    richiesta: str
    spostamenti: str
    note: str

    def __init__(self):
        pass

    def get_as_row(self, index: int) -> List:
        return [index + 1, self.regione, self.provincia, self.citta, self._get_spostamenti_citta(), self._get_spostamenti_provincia(), self._get_spostamenti_regione(), self.nome, self.cognome, self.email, self.telefono, self.note]

    def _get_spostamenti_citta(self) -> str:
        if self.spostamenti.startswith("Citt"):
            return "SI"
        return "NO"

    def _get_spostamenti_provincia(self) -> str:
        if self.spostamenti == "Provincia":
            return "SI"
        return "NO"

    def _get_spostamenti_regione(self) -> str:
        if self.spostamenti == "Regione":
            return "SI"
        return "NO"


def get_contacts(config: Dict) -> List[Contact]:
    from imbox import Imbox

    contacts: List[Contact] = []
    with Imbox(config["imap_url"], username=config["user"], password=config["password"],
               ssl=True, ssl_context=None, starttls=False) as imbox:

        # fetch all messages from inbox from receipt
        messages = imbox.messages(
            folder='INBOX', unread=False, sent_to=config["sent_to"])
        # read as html
        for msg_id, message in messages:
            contact = Contact()
            body = message.body['plain'][0]
            # extract string beetween 'Nome' and 'Cognome'
            contact.nome = body[body.find(
                'Nome')+5:body.find('Cognome')].strip()
            # extract string beetween 'Cognome' and 'Email'
            contact.cognome = body[body.find(
                'Cognome')+9:body.find('Email')].strip()
            # extract string beetween 'Email' and 'Telefono'
            contact.email = body[body.find(
                'Email')+7:body.find('Telefono')].strip()
            # extract string beetween 'Telefono' and 'Regione'
            contact.telefono = body[body.find(
                'Telefono')+10:body.find('Regione')].strip()
            # extract string beetween 'Regione' and 'Provincia'
            contact.regione = body[body.find(
                'Regione')+9:body.find('Provincia')].strip()
            # extract string beetween 'Provincia' and 'Città'
            contact.provincia = body[body.find('Provincia') +
                                     11:body.find('Citt&agrave;')].strip()
            # extract string beetween 'Città' and 'Richiesta'
            contact.citta = body[body.find('Citt&agrave;') +
                                 12:body.find('Richiesta')].strip()
            # extract string beetween 'Richiesta' and 'Spostamenti'
            contact.richiesta = body[body.find('Richiesta') +
                                     10:body.find('Spostamenti')].strip()
            # extract string beetween 'Spostamenti' and end of file
            contact.spostamenti = body[body.find('Spostamenti')+13:].strip()

            sent_at = datetime.strptime(message.date, '%a, %d %b %Y %X %z')
            contact.note = sent_at.strftime("%d/%m/%Y")

            contacts.append(contact)
            imbox.mark_seen(msg_id)

    return contacts


def import_contacts(contacts: List[Contact], config: Dict):
    import gspread

    gc = gspread.service_account(filename='service_account.json')
    sh = gc.open_by_key(config["sheet_id"])
    worksheet_list = sh.worksheets()
    worksheet_result = defaultdict(lambda: 0)
    for contact in contacts:
        worksheet = [
            w for w in worksheet_list if w.title == contact.regione]

        if (len(worksheet) != 0):
            print(f"${contact} no workseet matched")
        else:
            worksheet = worksheet[0]
            last_col_index = int(worksheet.col_values(1)[-1])
            worksheet.append_row(contact.get_as_row(last_col_index))
            worksheet_result[worksheet.title] += 1
    print(f"Contacts written for: ")
    for k, v in worksheet_result.items():
        print(f"{k}: {v}")


def run():
    with open('config.json', 'r') as f:
        config = json.load(f)
    now = datetime.now()
    print(f"Import started at {now}")
    contacts = get_contacts(config)
    number_of_contacts = len(contacts)
    print(f"Found {number_of_contacts} new contacts to import")
    import_contacts(contacts, config)
    now = datetime.now()
    print(f"Import ended at {now}")


scheduler = BlockingScheduler()
scheduler.add_job(run, 'interval', hours=6)
scheduler.start()
