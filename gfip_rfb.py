from collections import namedtuple
import re

"""
sections
========
a - gfip-file
b - gfip header  -- wont repeat within a
c - worker data  -- repeats within b

* a and b could be merged into a single section


fields to import:
=================
  - [a] CNPJ
  - [a] Competência

  - [b] Colaborador
  - [b] CNAE
  - [b] RAT
  - [b] FAP
  - [b] RAT Ajustado

  - [c] CBO
  - [c] PIS
  - [c] Admissão
  - [c] Categoria
  - [c] Base calculo INSS
  - [c] Base INSS 13°

how it works
============
struct FileData {
  turn_on_regex
  turn_off_regex
  // ..fields
}

/// worker data
struct Section {
    trigger_on_regex
    trigger_off_regex
    fields_to_catch: Vec<(Field, Option<Value>)>
    &FileData
}

struct Field {
    name
    regex
}


by the end of each worker section:
- verify that all fields were captured (have values), including file_data
- serialize section (print to stdout as csv)
- discard and create a new (idling) section

by the end of each file data:
- assert that there are no pending worker sections
-
"""

# data containers
Header = namedtuple('Header', ('cnpj competência colaborador cbo pis '
                               'admissao categoria base_inss base_inss_13'))
Field = namedtuple('Field', 'name regex')


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
                    self.fields[field_name] = catch

    def update_status(self, line):
        if self.is_active and self.off_regex.search(line):
            self.is_active = False
            results = self.flush()
            print(results)

            return False
        if not self.is_active and self.on_regex.search(line):
            self.is_active = True
            return True

    def flush(self):
        for required_field in self.fields_to_catch:
            if required_field.name not in self.fields:
                msg = "Flushed without catching all required fields"
                raise RuntimeError(msg)
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


def main(filepath):
    HeaderSectionWatcher = SectionWatcher(
        on_regex=HEADER_ON_REGEX,
        off_regex=HEADER_OFF_REGEX,
        fields_to_catch=HEADER_FIELDS_TO_CATCH,
    )

    hsw = HeaderSectionWatcher  # alias

    with open(filepath) as input_file:
        for line in input_file:
            hsw.update_status(line)
            hsw.consume(line)


if __name__ == '__main__':
    import sys
    filepath = sys.argv[1]
    main(filepath)
