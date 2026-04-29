# Latch

Latch é uma aplicação Flask para executar, acompanhar e interromper batches modulares compostos por plugins configuráveis.

O projeto fornece uma interface web simples para disparar comandos sequenciais por módulo, acompanhar logs em tempo real, evitar execuções concorrentes do mesmo módulo e interromper processos em execução quando necessário.

Página oficial: https://github.com/edgarrc/latch

## Propósito

Latch foi criado para centralizar a execução de rotinas batch locais de forma organizada e extensível.

Cada módulo define sua própria sequência de plugins em YAML. A aplicação carrega essa configuração, executa os plugins na ordem definida e interrompe o batch caso algum plugin falhe.

Casos de uso típicos:

- orquestrar scripts operacionais;
- executar rotinas analíticas em sequência;
- acompanhar logs de comandos longos via navegador;
- evitar execuções simultâneas acidentais;
- interromper processos manualmente quando necessário.

## Funcionalidades

- Módulos de usuário acessíveis por rota, como `/<id_do_modulo>`.
- Configuração de plugins por YAML.
- Criação e edição de módulos pela interface web.
- Validação de YAML antes de salvar.
- Variáveis por módulo com placeholders em comandos.
- Agendamento opcional por módulo usando cron de 5 campos.
- Suporte a variáveis de ambiente e valores sensíveis mascarados.
- Execução sequencial de plugins.
- Plugins `command_line`, `clickhouse_client` e `redis_client` para executar comandos no host.
- Captura de stdout/stderr em tempo real.
- Validação por exit code, string de erro e string de sucesso.
- Lock por módulo usando `filelock`.
- Logs persistidos da última execução em arquivos temporários do projeto.
- Limpeza dos logs/metadados temporários de módulos ao iniciar a aplicação.
- Recuperação do console ao sair e voltar para a página durante uma execução.
- Execução desacoplada da conexão SSE da página, evitando interrupção do batch por queda/reload do navegador.
- Status por etapa na página do módulo, com recuperação ao recarregar a tela.
- Atualização de status/logs por SSE global, sem polling periódico do navegador.
- Setup inicial com dois usuários fixos: `admin` e `user`.
- Apenas `admin` pode criar, editar, validar e excluir módulos; `user` pode visualizar o YAML/script do módulo em modo somente leitura.
- Botão `Clear` para limpar logs quando o módulo está parado.
- Botão `Kill` para interromper o plugin ativo.
- UI simples com Bootstrap via CDN.

## Stack

- Python
- Flask
- PyYAML
- filelock
- subprocess
- Server-Sent Events (SSE)
- Bootstrap
- pytest

## Estrutura

```text
.
├── app.py
├── modules/
│   ├── user/
│   │   └── *.yaml
│   └── system/
│       └── *.yaml
├── plugins/
│   ├── base.py
│   ├── clickhouse_client.py
│   ├── command_line.py
│   ├── redis_client.py
│   └── variables.py
├── templates/
│   ├── _app_footer.html
│   ├── _app_header.html
│   ├── login.html
│   ├── setup.html
│   ├── index.html
│   ├── module_edit.html
│   └── module.html
├── locks/
├── temp/
├── tests/
├── requirements.txt
├── AGENT.md
└── README.md
```

## Autenticação

Quando `settings.yaml` não existe, a aplicação abre o setup inicial e solicita uma senha para `admin` e outra para `user`.

O usuário `admin` pode operar batches e também criar, editar, validar e excluir módulos. O usuário `user` pode abrir módulos, executar batches, acompanhar status/logs, limpar logs quando permitido, solicitar `Kill` e visualizar o YAML/script do módulo em modo somente leitura, com valores `sensitive` mascarados.

O arquivo `settings.yaml` guarda apenas hashes das senhas e a chave de sessão da aplicação.

## Configuração de Módulos

Cada módulo de usuário é definido em `modules/user/<nome>.yaml`. O nome do arquivo, sem `.yaml`, é o ID do módulo e deve conter apenas letras, números, `_` ou `-`.

Exemplo:

```yaml
name: Analítico
description: Executa as etapas analíticas do batch.
schedule_enabled: true
schedule: "0 * * * *"
plugins:
  - id: preparar_analitico
    type: command_line
    description: Prepara o ambiente analítico.
    command: "echo Preparando modulo analitico"
    error_contains: "ERROR"
    success_contains: "analitico"

  - id: processar_analitico_com_sleep
    type: command_line
    description: Processa o batch analítico e valida a conclusão.
    command: "sleep 30 && echo Batch analitico concluido"
    error_contains: "ERROR"
    success_contains: "concluido"
```

Use `description` no módulo para explicar o objetivo geral e em cada plugin para explicar o que a etapa faz.

Use `schedule` opcionalmente para executar o módulo de forma automática. O valor deve ser uma string cron clássica de 5 campos, como `"0 * * * *"` para executar de hora em hora. Use `schedule_enabled: false` para manter o cron configurado sem executar automaticamente. O cron é interpretado no fuso local do servidor. Se o Latch estiver desligado ou o módulo ainda estiver rodando no horário agendado, a execução perdida não é reposta; o scheduler aguarda o próximo horário.

Módulos agendados continuam permitindo execução manual quando estão parados. Durante uma execução agendada, a página do módulo usa os mesmos controles de uma execução manual: `Executar` e `Clear` ficam bloqueados, `Kill` continua disponível e o console pode ser reaberto acompanhando os logs persistidos.

### Variáveis de Módulo

Um módulo pode declarar `variables:` para reutilizar valores nos comandos. O comando usa placeholders no formato `{nome_da_variavel}`.

Exemplo:

```yaml
name: Analítico
variables:
  database:
    type: string
    value: analytics
  batch_limit:
    type: integer
    value: 1000
  clickhouse_password:
    type: sensitive
    value: $CLICKHOUSE_PASSWORD
plugins:
  - id: consultar_clickhouse
    type: command_line
    command: "clickhouse-client --database {database} --password {clickhouse_password} --query 'SELECT * FROM eventos LIMIT {batch_limit}'"
    error_contains: "ERROR"
```

Exemplo 2:

```yaml
name: Módulo de exemplo
description: Demonstra variáveis string, integer e sensitive em comandos.
variables:
  nome_rotina:
    type: string
    value: analitico
  limite_linhas:
    type: integer
    value: 1000
  senha_demo:
    type: sensitive
    value: segredo_analitico_demo
plugins:
- id: echo_variavel_string
  type: command_line
  description: Exibe uma variável textual substituída no comando.
  command: 'echo Variavel string: {nome_rotina}'
  error_contains: ERROR
  success_contains: analitico
- id: echo_variavel_integer
  type: command_line
  description: Exibe uma variável inteira substituída no comando.
  command: 'echo Variavel integer: {limite_linhas}'
  error_contains: ERROR
  success_contains: '1000'
- id: echo_variavel_sensitive
  type: command_line
  description: Executa comando com variável sensível mascarada nos logs.
  command: 'echo Variavel sensitive: {senha_demo}'
  error_contains: ERROR
  success_contains: segredo_analitico_demo
- id: sleep
  type: command_line
  description: Simula uma etapa longa 10s.
  command: sleep 10
  error_contains: null
  success_contains: null
- id: sleep2
  type: command_line
  description: Simula uma segunda etapa longa.
  command: sleep 10
  error_contains: null
  success_contains: null
```

Tipos suportados:

- `string`: valor textual.
- `integer`: valor inteiro, aceitando inteiro no YAML ou texto numérico.
- `sensitive`: valor textual que não deve aparecer em logs, metadados ou console.

Quando `value` estiver no formato `$NOME_ENV`, a aplicação resolve o valor a partir da variável de ambiente `NOME_ENV` no momento da execução. Se a variável de ambiente não existir, o plugin falha antes de iniciar o comando.

Regras importantes:

- Variáveis são declaradas no escopo do módulo e podem ser usadas pelos plugins do módulo.
- Nomes de variáveis devem começar com letra ou `_` e conter apenas letras, números e `_`.
- Placeholders sem variável configurada fazem a execução falhar antes do comando iniciar.
- Em comandos string, valores substituídos são escapados com `shlex.quote` antes de executar com `shell=True`.
- Em comandos lista, cada item é executado como argumento direto, sem shell.
- Valores `sensitive` são substituídos por `****` nos logs e metadados. O mascaramento é literal: se um processo externo transformar o segredo, por exemplo usando hash ou encoding, essa transformação não é inferida.
- Para usar chaves literais em um comando quando `variables:` estiver configurado, escape como `{{` e `}}`.

### Plugins

#### `command_line`

Executa um comando direto ou via shell, conforme o tipo de `command`.
Opcionalmente, `pipeline` conecta a saída do comando principal a um shell raw no
lado direito do pipe, executado com Bash `pipefail`.

```yaml
plugins:
  - id: executar_script
    type: command_line
    command:
      - /usr/bin/python3
      - /opt/scripts/rotina.py
    pipeline: "grep concluido"
    error_contains: ERROR
    success_contains: concluido
```

#### `clickhouse_client`

Executa `/usr/bin/clickhouse-client` com argumentos montados pela aplicação. O campo `query` é obrigatório. `user`, `password`, `database` e `pipeline` são opcionais. A senha é sempre mascarada em logs e metadados, mesmo quando não vier de uma variável `sensitive`.

```yaml
plugins:
  - id: consultar_clickhouse
    type: clickhouse_client
    user: "{clickhouse_user}"
    password: "{clickhouse_password}"
    database: "{clickhouse_database}"
    query: SELECT COUNT(*) FROM relat_base_avaliacao_resposta
    pipeline: "grep -v '^0$'"
    error_contains: ERROR
    success_contains: null
```

Sem `pipeline`, o comando final é executado sem shell:

```text
/usr/bin/clickhouse-client --user ... --password ... --database ... --query ...
```

Com `pipeline`, o comando é conectado ao lado direito informado e executado via
`/bin/bash -o pipefail -c`, falhando se qualquer etapa do pipeline falhar.

#### `redis_client`

Executa `/usr/bin/redis-cli` com host e argumentos definidos pela configuração. O campo `host` é opcional. O campo `args` é obrigatório e pode ser lista de argumentos ou uma string interpretada com `shlex.split`. O campo `pipeline` é opcional e representa o shell raw à direita do pipe.

```yaml
plugins:
  - id: scan_redis
    type: redis_client
    host: "{redis_host}"
    args:
      - --scan
      - --pattern
      - exp_superset_data_*
    pipeline: "xargs redis-cli -h {redis_host} del"
    error_contains: ERROR
    success_contains: null
```

Sem `pipeline`, o comando final é executado sem shell:

```text
/usr/bin/redis-cli -h <host> <args...>
```

Com `pipeline`, o comando montado é convertido para shell com `shlex.join(...)` e
conectado ao `pipeline`. O `host` do `redis_client` não é propagado
automaticamente para esse lado direito; informe-o explicitamente no `pipeline`
quando necessário.

## Executando Localmente

Crie e ative um ambiente virtual:

```bash
python3 -m venv venv
source venv/bin/activate
```

Instale as dependências:

```bash
pip install -r requirements.txt
```

Inicie a aplicação:

```bash
flask --app app run

#OU

flask --app app run --host 0.0.0.0 --port 5000
```

Acesse:

```text
http://127.0.0.1:5000
```

## Testes

```bash
pytest
```

## Extensibilidade

Novos plugins devem herdar de `BasePlugin` e implementar:

- `run()`: executa o plugin e emite eventos de log.
- `kill()`: interrompe a execução ativa do plugin.

O contrato completo e as regras de arquitetura estão documentados em `AGENT.md`.

## Edição de Módulos

A página principal possui o botão `Adicionar` e cada linha possui `Editar` apenas para o usuário `admin`. Para `user`, a linha mostra `Ver script`, abrindo o YAML do módulo em modo somente leitura.

A tela de edição trabalha com YAML bruto e oferece:

- `Validate`: valida sintaxe YAML, campos obrigatórios, tipos de plugin, variáveis e placeholders sem persistir e sem reformatar o conteúdo.
- `Salvar`: valida novamente e grava o YAML bruto em `modules/user/<nome>.yaml`, preservando blocos literais, aspas, espaçamento e ordem informados no editor.
- `Excluir`: remove o YAML do módulo e arquivos temporários relacionados.

Criação, validação, salvamento e exclusão são ações exclusivas do `admin`. O usuário `user` pode visualizar o YAML, mas não valida nem altera a configuração. O salvamento e a exclusão de um módulo em execução são bloqueados para evitar mudança de configuração durante o batch.

## Status das Etapas

Na página do módulo, cada plugin aparece com um status individual:

- `Não iniciado`: antes da execução ou após limpar os logs.
- `Enfileirado`: etapa futura dentro do batch atual.
- `Executando`: etapa ativa.
- `Concluído`: etapa finalizada com sucesso.
- `Falhou`: etapa que interrompeu o batch por erro.
- `Interrompido`: etapa ativa quando o usuário solicitou `Kill`.

Esses estados são reconstruídos pelos eventos persistidos da execução, então continuam aparecendo corretamente ao recarregar a página durante ou após um batch.

## Observações de Segurança

Os plugins de execução rodam comandos no host. Portanto, os arquivos YAML devem ser tratados como configuração confiável.

Use `sensitive` para senhas, tokens e segredos. Não coloque segredos diretamente em `string` ou `integer`, pois esses valores podem aparecer nos logs.

Não exponha esta aplicação publicamente sem autenticação, autorização e revisão de segurança adequadas.
