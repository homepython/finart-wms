import React, { useState } from 'react';
import { StyleSheet, Text, View, TextInput, TouchableOpacity, Alert, ScrollView } from 'react-native';
import { CameraView, useCameraPermissions } from 'expo-camera';

// Substitua pelo IP do seu PC na rede Wi-Fi (veja com ipconfig no terminal) ou link online (Render/Railway)
const SERVIDOR_URL = 'http://192.168.1.100:3000';

export default function App() {
  const [permission, requestPermission] = useCameraPermissions();
  const [scannerAtivo, setScannerAtivo] = useState(true);
  const [dadosBipe, setDadosBipe] = useState(null);
  const [qtdVolumesEditavel, setQtdVolumesEditavel] = useState('');
  const [volumeEtiqueta, setVolumeEtiqueta] = useState('');

  if (!permission) return <View />;
  if (!permission.granted) {
    return (
      <View style={styles.container}>
        <Text style={styles.title}>FINART COLETOR</Text>
        <TouchableOpacity style={styles.btnConfirm} onPress={requestPermission}>
          <Text style={styles.btnText}>Liberar Câmera do Coletor</Text>
        </TouchableOpacity>
      </View>
    );
  }

  const aoBiparCodigo = async ({ data }) => {
    if (!scannerAtivo) return;
    setScannerAtivo(false);

    try {
      const response = await fetch(`${SERVIDOR_URL}/api/interpretar-bipe`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ codigoLido: data })
      });

      const resData = await response.json();
      if (!response.ok) {
        Alert.alert('Erro na Leitura', resData.erro || 'Etiqueta não reconhecida', [
          { text: 'Bipar Novamente', onPress: () => setScannerAtivo(true) }
        ]);
        return;
      }

      setDadosBipe(resData);
      setQtdVolumesEditavel(String(resData.qtd_volumes_sugerida));
      setVolumeEtiqueta('');
    } catch (error) {
      Alert.alert('Falha de Conexão', 'Verifique se o servidor FINART está ligado.', [
        { text: 'Tentar Novamente', onPress: () => setScannerAtivo(true) }
      ]);
    }
  };

  const confirmarEnvioPalete = async () => {
    const volumesNum = parseInt(qtdVolumesEditavel, 10);
    if (!volumesNum || volumesNum <= 0) {
      Alert.alert('Atenção', 'Informe uma quantidade válida de caixas/sacaria.');
      return;
    }

    try {
      const response = await fetch(`${SERVIDOR_URL}/api/paletes`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          ...dadosBipe,
          qtd_volumes_palete: volumesNum,
          volume_etiqueta: volumeEtiqueta
        })
      });

      const resultado = await response.json();
      if (!response.ok) {
        Alert.alert('Bloqueio de Segurança', resultado.erro);
        return;
      }

      Alert.alert(
        '✅ Palete Registrado - FINART',
        `Lote: ${dadosBipe.lote}\nSaldo Adicionado: ${resultado.saldo_total_pecas} peças`,
        [{ text: 'Próximo Palete', onPress: () => { setDadosBipe(null); setScannerAtivo(true); } }]
      );
    } catch (err) {
      Alert.alert('Erro', 'Não foi possível enviar o saldo ao servidor.');
    }
  };

  const saldoCalculadoAgora = dadosBipe
    ? (parseInt(qtdVolumesEditavel || '0', 10) * dadosBipe.qtd_pecas_por_volume)
    : 0;

  return (
    <View style={styles.container}>
      <View style={styles.header}>
        <Text style={styles.brand}>FINART</Text>
        <Text style={styles.subtitle}>Coletor WMS Online</Text>
      </View>

      {scannerAtivo ? (
        <View style={styles.cameraBox}>
          <CameraView
            style={StyleSheet.absoluteFillObject}
            barcodeScannerSettings={{ barcodeTypes: ['qr', 'code128', 'ean13'] }}
            onBarcodeScanned={aoBiparCodigo}
          />
          <View style={styles.mira} />
          <Text style={styles.instrucao}>Aponte para o Código de Barras ou QR Code da etiqueta</Text>
        </View>
      ) : (
        dadosBipe && (
          <ScrollView style={styles.cardConfirmacao}>
            <Text style={styles.cardTitle}>Confirmar Palete Bipado</Text>
            <Text style={styles.infoLine}>Cliente: <Text style={styles.bold}>{dadosBipe.cliente}</Text></Text>
            <Text style={styles.infoLine}>Pedido: <Text style={styles.bold}>{dadosBipe.pedido}</Text> | Lote: <Text style={styles.loteText}>{dadosBipe.lote}</Text></Text>
            <Text style={styles.infoLine}>Item: <Text style={styles.bold}>{dadosBipe.item}</Text></Text>
            <Text style={styles.infoLine}>Produto: {dadosBipe.produto}</Text>
            <Text style={styles.infoLine}>Peças por {dadosBipe.tipoEmbalagem}: <Text style={styles.bold}>{dadosBipe.qtd_pecas_por_volume} un.</Text></Text>

            <Text style={styles.labelInput}>
              Qtd de {dadosBipe.tipoEmbalagem}S no Palete (Padrão sugerido: {dadosBipe.qtd_volumes_sugerida}):
            </Text>
            <TextInput
              style={styles.inputDestaque}
              keyboardType="numeric"
              value={qtdVolumesEditavel}
              onChangeText={setQtdVolumesEditavel}
            />

            <Text style={styles.labelInput}>Nº Volume da Etiqueta (Opcional - ex: 029/125 p/ evitar duplicidade):</Text>
            <TextInput
              style={styles.inputSecundario}
              placeholder="Ex: 029/125"
              placeholderTextColor="#64748b"
              value={volumeEtiqueta}
              onChangeText={setVolumeEtiqueta}
            />

            <View style={styles.boxSaldoTotal}>
              <Text style={styles.saldoLabel}>SALDO TOTAL DESTE PALETE:</Text>
              <Text style={styles.saldoValor}>{saldoCalculadoAgora.toLocaleString('pt-BR')} PEÇAS</Text>
            </View>

            <TouchableOpacity style={styles.btnConfirm} onPress={confirmarEnvioPalete}>
              <Text style={styles.btnText}>CONFIRMAR E SUBIR SALDO</Text>
            </TouchableOpacity>

            <TouchableOpacity style={styles.btnCancel} onPress={() => { setDadosBipe(null); setScannerAtivo(true); }}>
              <Text style={styles.btnText}>Cancelar / Bipar Outra</Text>
            </TouchableOpacity>
          </ScrollView>
        )
      )}
    </View>
  );
}

const styles = StyleSheet.create({
  container: { flex: 1, backgroundColor: '#0f172a', paddingTop: 45 },
  header: { paddingHorizontal: 20, paddingBottom: 15, borderBottomWidth: 1, borderColor: '#1e293b', flexDirection: 'row', justifyContent: 'space-between', alignItems: 'center' },
  brand: { color: '#06b6d4', fontSize: 24, fontWeight: '900', letterSpacing: 2 },
  subtitle: { color: '#94a3b8', fontSize: 13 },
  cameraBox: { flex: 1, justifyContent: 'center', alignItems: 'center' },
  mira: { width: 280, height: 160, borderWidth: 2, borderColor: '#06b6d4', borderRadius: 12, backgroundColor: 'rgba(6, 182, 212, 0.08)' },
  instrucao: { color: '#fff', backgroundColor: 'rgba(15,23,42,0.85)', padding: 12, borderRadius: 8, marginTop: 20, fontWeight: '600' },
  cardConfirmacao: { flex: 1, padding: 20 },
  cardTitle: { color: '#06b6d4', fontSize: 20, fontWeight: 'bold', marginBottom: 12 },
  infoLine: { color: '#e2e8f0', fontSize: 15, marginBottom: 6 },
  bold: { fontWeight: 'bold', color: '#fff' },
  loteText: { fontWeight: 'bold', color: '#f59e0b', fontSize: 16 },
  labelInput: { color: '#94a3b8', fontSize: 13, marginTop: 14, marginBottom: 6 },
  inputDestaque: { backgroundColor: '#1e293b', color: '#10b981', fontSize: 26, fontWeight: 'bold', padding: 12, borderRadius: 8, borderWidth: 1, borderColor: '#06b6d4', textAlign: 'center' },
  inputSecundario: { backgroundColor: '#1e293b', color: '#fff', fontSize: 16, padding: 10, borderRadius: 8, borderWidth: 1, borderColor: '#334155' },
  boxSaldoTotal: { backgroundColor: '#1e293b', padding: 16, borderRadius: 10, marginVertical: 18, alignItems: 'center', borderWidth: 1, borderColor: '#10b981' },
  saldoLabel: { color: '#94a3b8', fontSize: 12 },
  saldoValor: { color: '#10b981', fontSize: 28, fontWeight: '900', marginTop: 4 },
  btnConfirm: { backgroundColor: '#10b981', padding: 16, borderRadius: 10, alignItems: 'center', marginBottom: 12 },
  btnCancel: { backgroundColor: '#ef4444', padding: 14, borderRadius: 10, alignItems: 'center', marginBottom: 30 },
  btnText: { color: '#fff', fontWeight: 'bold', fontSize: 15 }
});