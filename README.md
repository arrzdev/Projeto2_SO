# TODO

- [x] Implementar as interfaces de linha de comando (CLI) dos clientes

- [x] Implementar a serialização do protocolo de comunicação

- [ ] Implementar uma versão básica do mbroker, onde só existe uma thread que, em ciclo

  - [ ] recebe um pedido de registo
  - [ ] trata a sessão correspondente
  - [ ] volta a ficar à espera do pedido de registo

- [ ] Implementar a fila produtor-consumidor

  - [ ] Utilizar a fila produtor-consumidor para gerir e encaminhar os pedidos de registo para as worker threads.
