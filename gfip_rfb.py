from collections import namedtuple
import re

# data containers
Field = namedtuple('Field', 'name regex')
Header = namedtuple('Header', ('cnpj competência colaborador cbo pis '
                               'admissao categoria base_inss base_inss_13'))

allowed_to_be_empty = (
    'data_adm',  # autonomous workers don't have admission date
)


month_names = {
    'Janeiro': 1,
    'Fevereiro': 2,
    'Março': 3,
    'Abril': 4,
    'Maio': 5,
    'Junho': 6,
    'Julho': 7,
    'Agosto': 8,
    'Setembro': 9,
    'Outubro': 10,
    'Novembro': 11,
    'Dezembro': 12,
    '13° mês': 13,
}


def fix_date(dictionary):
    month, year = dictionary.pop('competencia').split(' de ')
    dictionary['mes'] = month_names[month]
    dictionary['ano'] = int(year)
    return dictionary


def catch_field(field, text):
    match = field.regex.search(text)
    if match:
        return (field.name, match.groups()[0])


class SectionWatcher():
    def __init__(self, on_regex, off_regex, fields_to_catch):
        self.on_regex = on_regex
        self.off_regex = off_regex
        self.fields_to_catch = fields_to_catch
        self.is_active = False
        self.fields = dict()

    def consume(self, line):
        if self.is_active:
            for field in self.fields_to_catch:
                catch = catch_field(field, line)
                if catch:
                    (field_name, catched_value) = catch
                    catched = catched_value.strip()
                    if not catched and field_name not in allowed_to_be_empty:
                        raise RuntimeError("Empty field")
                    self.fields[field_name] = catched

    def update_status(self, line):
        # Turn off and flush
        if self.is_active and self.off_regex.search(line):
            self.is_active = False
            return self.flush()

        # Turn on
        if not self.is_active and self.on_regex.search(line):
            self.is_active = True

    def flush(self):
        # handle missing fields
        missing = [f.name for f in self.fields_to_catch
                   if f.name not in self.fields]
        if len(missing) > 1:
            msg = "Flushed without catching all required fields: {missing}"
            raise RuntimeError(msg)
        elif missing:
            key, = missing
            self.fields[key] = '#[missing]'

        fields = self.fields
        self.fields = dict()
        self.active = False
        return fields


HEADER_ON_REGEX = re.compile(r'^\d+ - GFIP$')
HEADER_OFF_REGEX = re.compile(r'Totais da GFIP')
HEADER_FIELDS_TO_CATCH = (
    Field("cnae"         , re.compile(r'Código CNAE Preponderante\s+(\d+)')),
    Field("cnpj"         , re.compile(r'CNPJ/CEI do Estabelecimento\s+(\d+.*)')),
    Field("competencia"  , re.compile(r'Mês\s+((?:\w+|13° mês) de \d{4})')),
    Field("fap"          , re.compile(r'FAP\s+(\d+,\d+)')),
    Field("rat"          , re.compile(r'RAT: Alíquota\s+(\d+,\d+)')),
    Field("rat_ajustado" , re.compile(r'RAT: Ajustado\s+(\d+,\d+)')),
)


WORKER_ON_REGEX = re.compile(r'^(?:\d+\.?){3} - Trabalhador\s+[\d.-]+\s[A-Z ]+$')
WORKER_OFF_REGEX = re.compile(r'Movimentações do Trabalhador')
WORKER_FIELDS_TO_CATCH = (
    Field('section'     , re.compile(r'^(\d+\.3\.\d+)\s')),
    Field('colaborador' , re.compile(r'Trabalhador.*?([A-Z][A-Z ]+)')),
    Field('cbo'         , re.compile(r'Classificação Brasileira de Ocupações \(CBO\)\s+(\d+)')),
    Field('pis'         , re.compile(r'NIT do Trabalhador\s+(\d.*)')),
    Field('data_adm'    , re.compile(r'Dia Admissão\s+(.*)')),
    Field('categoria'   , re.compile(r'Código da Categoria\s+(\d+)')),
    Field('bc_inss'     , re.compile(r'Valor base de cálculo mensal\s+(\d.*)')),
    Field('bc_inss_13'  , re.compile(r'Valor base de cálculo 13º\s+(\d.*)')),
)


def scan(filepath):
    HeaderSectionWatcher = SectionWatcher(
        on_regex=HEADER_ON_REGEX,
        off_regex=HEADER_OFF_REGEX,
        fields_to_catch=HEADER_FIELDS_TO_CATCH,
    )
    WorkerSectionWatcher = SectionWatcher(
        on_regex=WORKER_ON_REGEX,
        off_regex=WORKER_OFF_REGEX,
        fields_to_catch=WORKER_FIELDS_TO_CATCH,
    )

    # aliases
    hsw = HeaderSectionWatcher
    wsw = WorkerSectionWatcher

    last_header = None

    with open(filepath) as input_file:
        for _idx, line in enumerate(input_file, start=1):
            header_result = hsw.update_status(line)
            worker_result = wsw.update_status(line)

            hsw.consume(line)
            wsw.consume(line)

            if worker_result:
                assert last_header is not None
                yield({**worker_result, **last_header})
            if header_result:
                last_header = fix_date(header_result)


if __name__ == '__main__':
    import sys
    import csv

    filepath = sys.argv[1]
    headers = ('ano mes cnpj cnae rat fap rat_ajustado colaborador pis '
               'cbo categoria data_adm bc_inss bc_inss_13 section').split()
    writer = csv.DictWriter(sys.stdout, headers)
    writer.writeheader()
    for record in scan(filepath):
        writer.writerow(record)
