package main

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	MQTT "github.com/eclipse/paho.mqtt.golang"
	"github.com/google/uuid"
)

type Schema struct {
	Name     string                 `json:"name"`
	Tags     Tags                   `json:"tags"`
	Fields   map[string]interface{} `json:"fields"`
	DeviceId string                 `json:"deviceId"`
}

type Tags struct {
	Organization   string `json:"organization"`
	DeviceType     string `json:"deviceType"`
	DeviceName     string `json:"deviceName"`
	DeviceId       string `json:"deviceId"`
	DeviceLocation string `json:"deviceLocation"`
}

type Influx struct {
	Measurement string `json:"measurement"`
	Tags        any    `json:"tags"`
	Fields      any    `json:"fields"`
	Timestamp   uint64 `json:"timestamp"`
}
type LnsUp struct {
	Measurement        string  // `json:"measurement"`
	DeviceId           string  // `json:"deviceId"`
	RxInfoMac_0        string  // `json:"rxInfo_mac_0"`
	RxInfoTime_0       int64   // `json:"rxInfo_time_0"`
	RxInfoRssi_0       int64   // `json:"rxInfo_rssi_0"`
	RxInfoSnr_0        float64 // `json:"rxInfo_snr_0"`
	RxInfoLat_0        float64 // `json:"rxInfo_lat_0"`
	RxInfoLon_0        float64 // `json:"rxInfo_lon_0"`
	RxInfoAlt_0        uint64  // `json:"rxInfo_alt_0"`
	TxInfoFrequency    float64 // `json:"txInfo_frequency"`
	TxInfoModulation   string  // `json:"txInfo_modulation"`
	TxInfoBandWidth    uint64  // `json:"txInfo_bandwidth"`
	TxInfoSpreadFactor uint64  // `json:"txInfo_spreadFactor"`
	// TxInfoCodeRate     string  `json:"txInfo_codeRate"`
	FCnt  uint64 `json:"fCnt"`
	FPort uint64 `json:"fPort"`
	FType string `json:"fType"`
	Data  string `json:"data"`
}

type Alert struct {
	DeviceId     string `json:"deviceId"`
	DeviceType   string `json:"deviceType"`
	Etc          string `json:"etc"`
	Data         string `json:"data"`
	Timestamp    int64  `json:"timestamp"`
	Trigger      string `json:"trigger"`
	TriggerAt    string `json:"triggerAt"`
	TriggerType  string `json:"triggerType"`
	LastPlayed   string `json:"lastPlayed"`
	ActionSensor string `json:"actionSensor"`
	CurrentValue string `json:"currentValue"`
}

type HealthPackUp struct {
	Props HealthPackUpProps      `json:"props"`
	Data  map[string]interface{} `json:"data"`
	Date  string                 `json:"date"`
}

type HealthPackUpProps struct {
	DeviceName string `json:"deviceName"`
	MacAddress string `json:"macAddress"`
	DeviceIp   string `json:"deviceIp"`
}

type HealthPackInertias struct {
	FAccX        string `json:"fAccX"`        // :"0.0"
	FAccY        string `json:"fAccY"`        // :"0.0"
	FAccZ        string `json:"fAccZ"`        // :"0.0"
	AccX         string `json:"accX"`         // :"944.000000"
	AccY         string `json:"accY"`         // :"-356.000000"
	AccZ         string `json:"accZ"`         // :"-16960.000000"
	GyrX         string `json:"gyrX"`         // :"-491.000000"
	GyrY         string `json:"gyrY"`         // :"15.000000"
	GyrZ         string `json:"gyrZ"`         // :"196.000000"
	ContimpactoX string `json:"contimpactoX"` // :"944.000000"
	ContimpactoY string `json:"contimpactoY"` // :"412.000000"
	ContimpactoZ string `json:"contimpactoZ"` // :"17968.000000"
	Pitch        string `json:"pitch"`        // :"-1.200725"
	Roll         string `json:"roll"`         // :"-3.185352"
	Yaw          string `json:"yaw"`          // :"0.000000"}
}

type HealthPackTracking struct {
	Latitude               string `json:"latitude"`               // :"0.000000"
	Longitude              string `json:"longitude"`              // :"0.000000"
	Tempbateriasecundaria  string `json:"tempbateriasecundaria"`  // :"24.2"
	Tempbateriaprincipal   string `json:"tempbateriaprincipal"`   // :"23.6"
	Temperaturacondensador string `json:"temperaturacondensador"` // :"28.1"
	Temperaturacuba1       string `json:"temperaturacuba1"`       // :"-0.9"
	Temperaturacuba2       string `json:"temperaturacuba2"`       // :"11.4"
	TemperaturaexternaLL   string `json:"temperaturaexternaLL"`   // :"36.3"
	TemperaturaexternaLS   string `json:"temperaturaexternaLS"`   // :"8.4"
	Temperaturaexterna     string `json:"temperaturaexterna"`     // :"28.1"
	Temperaturadissipador  string `json:"temperaturadissipador"`  // :"26.5"
	Correntebateria        string `json:"correntebateria"`        // :"0.0"
	Correntecompressor     string `json:"correntecompressor"`     // :"3.9"
	Correntepeltier        string `json:"correntepeltier"`        // :"0.0"
	Correntecooler         string `json:"correntecooler"`         // :"0.0"
	Correnteexaustor       string `json:"correnteexaustor"`       // :"0.0"
	Temperaturacompressor  string `json:"temperaturacompressor"`  // :"44.8"
	Setpoint_pid1          string `json:"setpoint_pid1"`          // :"-5.0"
	Valor_pid1_atual       string `json:"valor_pid1_atual"`       // :"-0.9"
	Esforco_pid1           string `json:"esforco_pid1"`           // :"536.0"
	Setpoint_pid2          string `json:"setpoint_pid2"`          // :"50.0"
	Valor_pid2_atual       string `json:"valor_pid2_atual"`       // :"44.8"
	Esforco_pid2           string `json:"esforco_pid2"`           // :"0.0"}
}

type HealthPackStatus struct {
	Vbateriaprincipal           string `json:"vbateriaprincipal"`           // :"21.8"
	Vbateriasecundaria          string `json:"vbateriasecundaria"`          // :"4.2"
	Ventradafonteexterna        string `json:"ventradafonteexterna"`        // :"21.8"
	Numerocaixa                 string `json:"numerocaixa"`                 // :"0.0"
	Estadomaquina               string `json:"estadomaquina"`               // :"53.0"
	Timestamp                   string `json:"timestamp"`                   // :"0.0"
	IdFalha                     string `json:"idFalha"`                     // :"0.0"
	Porcentagemsinalcomunicacao string `json:"porcentagemsinalcomunicacao"` // :"0.0"
	FatorRH                     string `json:"fatorRH"`                     // :"0.0"
	Btdown                      string `json:"btdown"`                      // :"0.0"
	Btselect                    string `json:"btselect"`                    // :"0.0"
	Btup                        string `json:"btup"`                        // :"0.0"
	Tecla_enter                 string `json:"tecla_enter"`                 // :"0.0"
	Statustampaprincipal        string `json:"statustampaprincipal"`        // :"1.0"
	Statusserialprincipal       string `json:"statusserialprincipal"`       // :"0.0"
	Statusserialsecundaria      string `json:"statusserialsecundaria"`      // :"0.0"
	Statustampacasamaq          string `json:"statustampacasamaq"`          // :"0.0"
	Controle_peltier            string `json:"controle_peltier"`            // :"0.0"
	Porcentagem_bat             string `json:"porcentagem_bat"`             // :"100.0"}
}

type HealthPackIschemia struct {
	IdModal                 string `json:"idModal"`
	IdOperador              string `json:"IdOperador"`              // :""
	Niveldepermissao        string `json:"niveldepermissao"`        // :""
	Nome                    string `json:"nome"`                    // :""
	Numtransplante          string `json:"numtransplante"`          // :""
	Numeroempresa           string `json:"numeroempresa"`           // :""
	Orgao                   string `json:"orgao"`                   // :"tecidos_oculares"
	Tempo_total_isquemia    string `json:"tempo_total_isquemia"`    // :"00/00/00 00:00:00"
	Tempo_restante_isquemia string `json:"tempo_restante_isquemia"` // :"00d 00:00"
	Hora_isquemia           string `json:"hora_isquemia"`           // :""
	Timeinfo_sp2            string `json:"timeinfo_sp2"`            // :"24:12:09 16:18:53"}
}

type HealthPackAlarm struct {
	Alarms map[string]interface{} `json:"alarms"`
}

type NspiUp struct {
	Measurement string `json:"measurement"`
	DeviceId    string `json:"deviceId"`
	DeviceType  string `json:"deviceType"`
	Data        string `json:"data"`
	Timestamp   int64  `json:"timestamp"`
}

type NspiGenericJson struct {
	Data string `json:"data"`
}
type EvseUp struct {
	FeatureName   string `json:"featureName"`
	DeviceId      string `json:"deviceId"`
	DeviceType    string `json:"deviceType"`
	ConnectorId   string `json:"ConnectorId"`
	ChargePointId string `json:"chargePointId"`
	Unit          string `json:"unit"`
	Format        string `json:"format"`
	Measurand     string `json:"measurand"`
	Context       string `json:"context"`
	Location      string `json:"location"`
	Timestamp     int64  `json:"timestamp"`
}

type EvseMeterValue struct {
	ForwardEnergy float64 `json:"forwardEnergy"`
}

type EvseStatusNotification struct {
	Status          string `json:"status"`
	ErrorCode       string `json:"errorCode"`
	Info            string `json:"info"`
	VendorId        string `json:"vendorId"`
	VendorErrorCode string `json:"vendorErrorCode"`
}

type EvseStartTransaction struct {
	StartMeter    int64  `json:"startMeter"`
	TransactionId string `json:"transactionId"`
	StartTime     int64  `json:"startTime"`
	IdTag         string `json:"idTag"`
}

type EvseStopTransaction struct {
	TransactionId string `json:"transactionId"`
	MeterStop     int64  `json:"meterStop"`
	StopTime      int64  `json:"stopTime"`
}
type LnsCommand struct {
	Measurement string
	Application string
	Reference   string
	DeviceId    string
	Confirmed   bool
	FPort       uint64
	Data        string
	Timestamp   int64
	// Object any
}

type LnsImtCommand struct {
	Reference string
	Confirmed bool
	FPort     uint64
	Data      string
}

type LnsChirpstackV4Command struct {
	DeviceId  string
	Confirmed bool
	FPort     uint64
	Data      string
	// Object any
}

type Port100 struct {
	X_01_0 float64 `json:"01_0"`
	X_01_1 float64 `json:"01_1"`
	X_01_2 float64 `json:"01_2"`
	X_01_3 float64 `json:"01_3"`
	X_01_4 float64 `json:"01_4"`
	X_01_5 float64 `json:"01_5"`
	X_01_6 float64 `json:"01_6"`
	X_01_7 float64 `json:"01_7"`
	X_02   float64 `json:"02"`
	X_03_0 float64 `json:"03_0"`
	X_03_1 float64 `json:"03_1"`
	X_04   uint64  `json:"04"`
	X_05_0 float64 `json:"05_0"`
	X_05_1 float64 `json:"05_1"`
	X_05_2 float64 `json:"05_2"`
	X_06   uint64  `json:"06"`
	X_07   uint64  `json:"07"`
	X_08   uint64  `json:"08"`
	X_09   uint64  `json:"09"`
	X_0A_0 float64 `json:"0A_0"`
	X_0A_1 float64 `json:"0A_1"`
	X_0B   uint64  `json:"0B"`
	X_0C   float64 `json:"0C"`
	X_0D_0 uint64  `json:"0D_0"`
	X_0D_1 uint64  `json:"0D_1"`
	X_0D_2 uint64  `json:"0D_2"`
	X_0D_3 uint64  `json:"0D_3"`
	X_0E_0 float64 `json:"0E_0"`
	X_0E_1 float64 `json:"0E_1"`
	X_10   uint64  `json:"10"`
	X_11   float64 `json:"11"`
	X_12   uint64  `json:"12"`
	X_13   uint64  `json:"13"`
}

type Port4 struct {
	InternalBatteryVoltage float64
	PowerSource            bool
	FirmwareVersion        uint64
	EnvSensorFailStatus    bool
	C1State                bool
	C1Count                uint64
	C2State                bool
	C2Count                uint64
	InternalTemperature    float64
	InternalHumidity       float64
	EmwRainLevel           float64
	EmwAvgWindSpeed        uint64
	EmwGustWindSpeed       uint64
	EmwWindDirection       uint64
	EmwTemperature         float64
	EmwHumidity            uint64
	EmwLuminosity          uint64
	EmwUv                  float64
	EmwSolarRadiation      float64
	EmwAtmPres             float64

	IsEnvSensorFailStatus    bool
	IsInternalBatteryVoltage bool
	IsFirmwareVersion        bool
	IsInternalTemperature    bool
	IsInternalHumidity       bool
	IsC1State                bool
	IsC1Count                bool
	IsC2State                bool
	IsC2Count                bool
	IsEmwRainLevel           bool
	IsEmwAvgWindSpeed        bool
	IsEmwGustWindSpeed       bool
	IsEmwWindDirection       bool
	IsEmwTemperature         bool
	IsEmwHumidity            bool
	IsEmwLuminosity          bool
	IsEmwUv                  bool
	IsEmwSolarRadiation      bool
	IsEmwAtmPres             bool
}
type SmartLight struct {
	Temperature    float64 `json:"temperature"`
	Humidity       float64 `json:"humidity"`
	Luminosity     float64 `json:"lux"`
	Movement       uint64  `json:"movement"`
	BatteryVoltage float64 `json:"battery"`
	BoardVoltage   float64 `json:"boardVoltage"`
}

type VibrationAverage struct {
	Temperature       float64 `json:"temperature"`
	Humidity          float64 `json:"humidity"`
	VibrationAverageX float64 `json:"lux"`
	VibrationAverageY float64 `json:"movement"`
	VibrationAverageZ float64 `json:"battery"`
	BoardVoltage      float64 `json:"boardVoltage"`
}

type WaterTankLevel struct {
	Distance     uint64  `json:"distance"`
	BoardVoltage float64 `json:"boardVoltage"`
}

type WeatherStation struct {
	InternalBatteryVoltage float64
	FirmwareVersion        uint64
	EnvSensorFailStatus    bool
	C1State                bool
	C1Count                uint64
	C2State                bool
	C2Count                uint64
	InternalTemperature    float64
	InternalHumidity       float64
	EmwRainLevel           float64
	EmwAvgWindSpeed        uint64
	EmwGustWindSpeed       uint64
	EmwWindDirection       uint64
	EmwTemperature         float64
	EmwHumidity            uint64
	EmwLuminosity          uint64
	EmwUv                  float64
	EmwSolarRadiation      float64
	EmwAtmPres             float64
	PowerSource            bool
}

type GaugePressure struct {
	InletPressure  float64 `json:"outletPressure"`
	OutletPressure float64 `json:"inletPressure"`
	BoardVoltage   float64 `json:"boardVoltage"`
}

type Hydrometer struct {
	Counter      uint64  `json:"counter"`
	BoardVoltage float64 `json:"boardVoltage"`
}

type EnergyMeter struct {
	ForwardEnergy float64 `json:"forwardEnergy"`
	ReverseEnergy float64 `json:"reverseEnergy"`
	BoardVoltage  float64 `json:"boardVoltage"`
}

type Sprinkler struct {
	Solenoid1    bool    `json:"solenoid1"`
	Solenoid2    bool    `json:"solenoid2"`
	Solenoid3    bool    `json:"solenoid3"`
	Counter      uint64  `json:"counter"`
	BoardVoltage float64 `json:"boardVoltage"`
}

type SoilMoisture3DepthLevels struct {
	SoilMoistureDepthLevel1 uint64  `json:"soilMoistureDepthLevel1"`
	SoilMoistureDepthLevel2 uint64  `json:"soilMoistureDepthLevel2"`
	SoilMoistureDepthLevel3 uint64  `json:"soilMoistureDepthLevel3"`
	BoardVoltage            float64 `json:"boardVoltage"`
}

type Temperature8Point struct {
	Temperature1 float64 `json:"temperature1"`
	Temperature2 float64 `json:"temperature2"`
	Temperature3 float64 `json:"temperature3"`
	Temperature4 float64 `json:"temperature4"`
	Temperature5 float64 `json:"temperature5"`
	Temperature6 float64 `json:"temperature6"`
	Temperature7 float64 `json:"temperature7"`
	Temperature8 float64 `json:"temperature8"`
	BoardVoltage float64 `json:"boardVoltage"`
}

type LnsChirpStackV4Up struct {
	DeduplicationId string                      `json:"deduplicationId"`
	DeviceInfo      LnsChirpStackV4UpDeviceInfo `json:"deviceInfo"`
	DevAddr         string                      `json:"devAddr"`
	Adr             bool                        `json:"adr"`
	Dr              uint64                      `json:"dr"`
	FCnt            uint64                      `json:"fCnt"`
	FPort           uint64                      `json:"fPort"`
	Confirmed       string                      `json:"Confirmed"`
	RxInfo          []LnsChirpStackV4UpRxInfo   `json:"rxInfo"`
	TxInfo          LnsChirpStackV4UpTxInfo     `json:"txInfo"`
	Data            string                      `json:"data"`
}

type LnsChirpStackV4UpDeviceInfo struct {
	TenantId           string `json:"tenantId"`
	TenantName         string `json:"tenantName"`
	ApplicationId      string `json:"applicationId"`
	ApplicationName    string `json:"applicationName"`
	DeviceProfileId    string `json:"deviceProfileId"`
	DeviceProfileName  string `json:"deviceProfileName"`
	DeviceName         string `json:"deviceName"`
	DevEui             string `json:"devEui"`
	DeviceClassEnabled string `json:"deviceClassEnabled"`
	Tags               any    `json:"tags"`
}

type LnsChirpStackV4UpRxInfo struct {
	GatewayId         string                    `json:"gatewayId"`
	UplinkId          uint64                    `json:"uplinkId"`
	NsTime            time.Time                 `json:"nsTime"`
	TimeSinceGpsEpoch string                    `json:"timeSinceGpsEpoch"`
	Rssi              int64                     `json:"rssi"`
	Snr               float64                   `json:"snr"`
	Channel           uint64                    `json:"channel"`
	Board             uint64                    `json:"board"`
	Location          LnsChirpStackV4UpLocation `json:"location"`
	Context           string                    `json:"context"`
	Metadata          LnsChirpStackV4UpMetadata `json:"metadata"`
	CrcStatus         string                    `json:"crcStatus"`
}

type LnsChirpStackV4UpLocation struct {
	Latitude  float64 `json:"latitude"`
	Longitude float64 `json:"longitude"`
	Altitude  uint64  `json:"altitude"`
}

type LnsChirpStackV4UpMetadata struct {
	Region_config_id   string `json:"region_config_id"`
	Region_common_name string `json:"region_common_name"`
}

type LnsChirpStackV4UpTxInfo struct {
	Frequency  float64                     `json:"frequency"`
	Modulation LnsChirpStackV4UpModulation `json:"modulation"`
}

type LnsChirpStackV4UpModulation struct {
	Lora LnsChirpStackV4UpLora `json:"lora"`
}

type LnsChirpStackV4UpLora struct {
	Bandwidth       uint64 `json:"bandwidth"`
	SpreadingFactor uint64 `json:"spreadingFactor"`
	// CodeRate        string `json:"codeRate"`
}

type LnsImtUp struct {
	ApplicationID   string           `json:"applicationID"`
	ApplicationName string           `json:"applicationName"`
	NodeName        string           `json:"nodeName"`
	DevEUI          string           `json:"devEUI"`
	RxInfo          []LnsImtUpRxInfo `json:"rxInfo"`
	TxInfo          LnsImtUpTxInfo   `json:"txInfo"`
	FCnt            uint64           `json:"fCnt"`
	FPort           uint64           `json:"FPort"`
	Data            string           `json:"data"`
}

type LnsImtUpRxInfo struct {
	Mac       string    `json:"mac"`
	Time      time.Time `json:"time"`
	Rssi      int64     `json:"rssi"`
	LoRaSNR   float64   `json:"loRaSNR"`
	Name      string    `json:"name"`
	Latitude  float64   `json:"latitude"`
	Longitude float64   `json:"longitude"`
	Altitude  uint64    `json:"altitude"`
}

type LnsImtUpDataRate struct {
	Modulation   string `json:"modulation"`
	Bandwidth    uint64 `json:"bandwidth"`
	SpreadFactor uint64 `json:"spreadFactor"`
}

type LnsImtUpTxInfo struct {
	Frequency float64          `json:"frequency"`
	DataRate  LnsImtUpDataRate `json:"dataRate"`
	Adr       bool             `json:"adr"`
	// CodeRate  string         `json:"codeRate"`
}

func roundFloat(val float64, precision uint) float64 {
	ratio := math.Pow(10, float64(precision))
	return math.Round(val*ratio) / ratio
}

func protocolParserPort4(bytes []byte) string {
	var port4 Port4
	port4.IsInternalTemperature = false
	port4.IsInternalHumidity = false
	port4.IsEmwRainLevel = false
	port4.IsEmwAvgWindSpeed = false
	port4.IsEmwGustWindSpeed = false
	port4.IsEmwWindDirection = false
	port4.IsEmwTemperature = false
	port4.IsEmwHumidity = false
	port4.IsEmwLuminosity = false
	port4.IsEmwUv = false
	port4.IsEmwSolarRadiation = false
	port4.IsEmwAtmPres = false
	port4.IsEnvSensorFailStatus = false
	port4.IsInternalBatteryVoltage = false
	port4.IsFirmwareVersion = false
	port4.IsC1State = false
	port4.IsC1Count = false
	port4.IsC2State = false
	port4.IsC2Count = false

	var maskSensorInt byte
	var maskSensorIntE byte
	var maskSensorExt byte

	index := 0

	// deviceModel := "NIT 21LI"

	// Verify the presence os maskSensorInt in byte[0]
	maskSensorInt = bytes[index]
	// fmt.Printf("\nprotocolParserPort4 => maskSensorInt %b", maskSensorInt)
	index = index + 1
	// If Extended Internal Sensor Mask
	if maskSensorInt>>7&0x01 == 0x01 {
		maskSensorIntE = bytes[index]
		port4.IsEnvSensorFailStatus = true
		// fmt.Printf("\nprotocolParserPort4 => maskSensorIntE %b", maskSensorIntE)
		index = index + 1
	}

	// External Sensor Mask
	// byte [3] if Extended Internal Sensor Mask or byte [2] if does not
	maskSensorExt = bytes[index]
	// fmt.Printf("\nprotocolParserPort4 => maskSensorExt %b", maskSensorExt)
	index = index + 1

	// Verify Internal Humidity and Temperature fails
	if maskSensorIntE>>0&0x01 == 0x01 {
		// if 0x01&0x01 == 0x01 {
		port4.EnvSensorFailStatus = true
		// fmt.Printf("\nprotocolParserPort4 => EnvSensorFailStatus %s", port4.EnvSensorFailStatus)
	}

	// TODO: VERIFY CODE
	// Decode Battery
	// If bit 0 of maskSensorInt exists
	if maskSensorInt>>0&0x01 == 0x01 {
		port4.IsInternalBatteryVoltage = true
		if maskSensorInt>>6&0x01 == 0x01 {
			v := bytes[index]
			f := roundFloat(float64((v/120.0)+1), 2)
			port4.InternalBatteryVoltage = f
		} else {
			v := bytes[index]
			f := roundFloat(float64(v/10.0), 1)
			port4.InternalBatteryVoltage = f
		}
		index = index + 1
	}
	// fmt.Printf("\nprotocolParserPort4 => IsBattery %d", port4.IsBattery)

	// TODO: VERIFY CODE WITH 0xFE
	// Decode Firmware Version
	// Verify if firmware version appear on message
	if maskSensorInt>>2&0x01 == 0x01 {
		port4.IsFirmwareVersion = true
		v := uint64(bytes[index])
		v |= uint64(bytes[index+2]) << 8
		v |= uint64(bytes[index+3]) << 16
		v = v / 1000000
		// fmt.Printf("\nprotocolParserPort4 => Firmware Version %d", v)

		// hardware = (v / 1000000) >>> 0;
		// let compatibility = ((firmware.v / 10000) - (hardware * 100)) >>> 0;
		// let feature = ((firmware.v - (hardware * 1000000) - (compatibility * 10000)) / 100) >>> 0;
		// let bug = (firmware.v - (hardware * 1000000) - (compatibility * 10000) - (feature * 100)) >>> 0;
		// firmware.v = hardware + '.' + compatibility + '.' + feature + '.' + bug;
		// data.device.push(firmware);
		index = index + 3
	}
	// fmt.Printf("\nprotocolParserPort4 => IsFirmware %d", port4.IsFirmware)

	// Decode External Power or Battery
	if maskSensorInt>>5&0x01 == 0x01 {
		b := true
		port4.PowerSource = b
	} else {
		b := false
		port4.PowerSource = b
	}
	// fmt.Printf("\nprotocolParserPort4 => Power Source %d", port4.Power)

	// Decode Temperature Int
	if maskSensorInt>>3&0x01 == 0x01 {
		port4.IsInternalTemperature = true
		v := uint64(bytes[index])
		v |= uint64(bytes[index+1]) << 8
		f := roundFloat((float64(v)/100)-273.15, 2)
		port4.InternalTemperature = f
		index = index + 2
		// fmt.Printf("\nprotocolParserPort4 => Internal Temperature f %d", f)
	}

	// Decode Moisture Int
	if maskSensorInt>>4&0x01 == 0x01 {
		port4.IsInternalHumidity = true
		v := uint64(bytes[index])
		v |= uint64(bytes[index+1]) << 8
		f := roundFloat((float64(v) / 10), 2)
		port4.InternalHumidity = f
		index = index + 2
		// fmt.Printf("\nprotocolParserPort4 => Internal Humidity f %d", f)
	}

	// Decode Drys
	// Decode Dry 1 State
	if maskSensorExt>>0&0x01 == 0x01 {
		port4.IsC1State = true
		if bytes[index] == 0x01 {
			b := true
			port4.C1State = b
		} else {
			b := false
			port4.C1State = b
		}
		index = index + 1
		// fmt.Printf("\nprotocolParserPort4 => C1State %d", port4.C1State)
	}

	// Decode Dry 1 Count
	if maskSensorExt>>1&0x01 == 0x01 {
		port4.IsC1Count = true
		v := uint64(bytes[index])
		v |= uint64(bytes[index+1]) << 8
		port4.C1Count = v
		index = index + 2
		// fmt.Printf("\nprotocolParserPort4 => C1Count %d", port4.C1Count)
	}

	// Decode Dry 2 State
	if maskSensorExt>>2&0x01 == 0x01 {
		port4.IsC2State = true
		if bytes[index] == 0x01 {
			b := true
			port4.C2State = b
		} else {
			b := false
			port4.C2State = b
		}
		index = index + 1
		// fmt.Printf("\nprotocolParserPort4 => C2State %d", port4.C2State)
	}

	// Decode Dry 2 Count
	if maskSensorExt>>3&0x01 == 0x01 {
		port4.IsC2Count = true
		v := uint64(bytes[index])
		v |= uint64(bytes[index+1]) << 8
		port4.C2Count = v
		index = index + 2
		// fmt.Printf("\nprotocolParserPort4 => C2Count %d", port4.C2Count)
	}

	// // Decode DS18B20 Probe
	// if (mask_sensor_ext >> 4 & 0x07 == 0x07) {
	// 		let nb_probes = (mask_sensor_ext >> 4 & 0x07) >>> 0;
	// 		for (let i = 0; i < nb_probes; i++) {
	// 				let probe = { u: 'C' };
	// 				let rom = {};
	//
	// 				probe.v = (((input.bytes[index++] | (input.bytes[index++] << 8)) / 100.0) - 273.15).round(2);
	// 				if (mask_sensor_ext >> 7 & 0x01) {
	// 						index += 7;
	// 						rom = (input.bytes[index--]).toString(16);
	// 						for (let j = 0; j < 7; j++) {
	// 								rom += (input.bytes[index--]).toString(16);
	// 						}
	// 						index += 9;
	// 				} else {
	// 						rom = input.bytes[index++];
	// 				}
	// 				probe.n = 'temperature' + '_' + rom;
	// 				data.probes.push(probe);
	// 		}
	// }

	// Decode Extension Module(s) ONLY EMW104 validated
	if bytes[index] == 0x04 {

		switch bytes[index] {
		// 		case 1:
		// 			i = i + 1
		// 			maskEms104 := bytes[index+i]
		// 			i = i + 1

		// 			// E1
		// 			if maskEms104>>0&0x01 == 0x01 {
		// 				v := uint64(bytes[index])
		// 				v |= uint64(bytes[index+1] << 8)
		// 				f := float64(v / 100.0)
		// 				// .round(2)
		// 				port4.EmsE1Temp = f - 273.15
		// 			}

		// 			// KPA
		// 			// if (maskEms104 >> (k + 1) & 0x01) {
		// 			switch _kpa {
		// 			case 0:
		// 				v := uint64(bytes[i+3])
		// 				v |= uint64(bytes[i+4]) << 8
		// 				f := float64(v)
		// 				port100.Kpa_0 = f
		// 				i = i + 2
		// 				_kpa = _kpa + 1
		// 			case 1:
		// 				v := uint64(bytes[i+3])
		// 				v |= uint64(bytes[i+4]) << 8
		// 				f := float64(v)
		// 				port100.Kpa_1 = f
		// 				i = i + 2
		// 				_kpa = _kpa + 1
		// 			case 2:
		// 				v := uint64(bytes[i+3])
		// 				v |= uint64(bytes[i+4]) << 8
		// 				f := float64(v)
		// 				port100.Kpa_2 = f
		// 				i = i + 2
		// 				_kpa = _kpa + 1
		// 			}
		// 		case 2:

		// EM W104
		case 4:
			index = index + 1
			maskEmw104 := bytes[index] //0f
			index = index + 1
			// fmt.Printf("\nprotocolParserPort4 => maskEmw104 %d", maskEmw104)

			//Weather Station
			if maskEmw104>>0&0x01 == 0x01 {
				//Rain
				port4.IsEmwRainLevel = true
				v := uint64(bytes[index]) << 8
				v |= uint64(bytes[index+1])
				f := roundFloat((float64(v) / 10), 1)
				port4.EmwRainLevel = f
				index = index + 2
				// fmt.Printf("\nprotocolParserPort4 => EmwRainLevel %d", port4.EmwRainLevel)

				//Average Wind Speed
				port4.IsEmwAvgWindSpeed = true
				v = uint64(bytes[index])
				port4.EmwAvgWindSpeed = v
				// fmt.Printf("\nprotocolParserPort4 => EmwAvgWindSpeed %d", port4.EmwAvgWindSpeed)
				index = index + 1

				//Gust Wind Speed
				port4.IsEmwGustWindSpeed = true
				v = uint64(bytes[index])
				port4.EmwGustWindSpeed = v
				// fmt.Printf("\nprotocolParserPort4 => EmwGustWindSpeed %d", port4.EmwGustWindSpeed)
				index = index + 1

				//Wind Direction
				port4.IsEmwWindDirection = true
				v = uint64(bytes[index]) << 8
				v |= uint64(bytes[index+1])
				port4.EmwWindDirection = v
				// fmt.Printf("\nprotocolParserPort4 => EmwWindDirection %d", port4.EmwWindDirection)
				index = index + 2

				//Temperature
				port4.IsEmwTemperature = true
				v = uint64(bytes[index]) << 8
				v |= uint64(bytes[index+1])
				f = roundFloat((float64(v)/10)-273.15, 2)
				port4.EmwTemperature = f
				// fmt.Printf("\nprotocolParserPort4 => EmwTemperature %d", port4.EmwTemperature)
				index = index + 2

				//Humidity
				port4.IsEmwHumidity = true
				v = uint64(bytes[index])
				port4.EmwHumidity = v
				// fmt.Printf("\nprotocolParserPort4 => EmwHumidity %d", port4.EmwHumidity)
				index = index + 1
			}
			//Lux and UV
			if maskEmw104>>1&0x01 == 0x01 {
				port4.IsEmwLuminosity = true
				v := uint64(bytes[index]) << 16
				v |= uint64(bytes[index+1]) << 8
				v |= uint64(bytes[index+2])
				port4.EmwLuminosity = v
				// fmt.Printf("\nprotocolParserPort4 => EmwLuminosity %d", port4.EmwLuminosity)

				port4.IsEmwUv = true
				v = uint64(bytes[index+3])
				f := roundFloat((float64(v) / 10), 1)
				port4.EmwUv = f
				// fmt.Printf("\nprotocolParserPort4 => EmwUv %d", port4.EmwUv)
				index = index + 4
			}

			//Pyranometer
			if maskEmw104>>2&0x01 == 0x01 {
				port4.IsEmwSolarRadiation = true
				v := uint64(bytes[index]) << 8
				v |= uint64(bytes[index+1])
				f := roundFloat((float64(v) / 10), 1)
				port4.EmwSolarRadiation = f
				// fmt.Printf("\nprotocolParserPort4 => EmwSolarRadiation %d", port4.EmwSolarRadiation)
				index = index + 2
			}

			//Barometer
			if maskEmw104>>3&0x01 == 0x01 {
				port4.IsEmwAtmPres = true
				v := uint64(bytes[index]) << 16
				v |= uint64(bytes[index+1]) << 8
				v |= uint64(bytes[index+2])
				f := roundFloat((float64(v) / 100), 2)
				port4.EmwAtmPres = f
				// fmt.Printf("\nprotocolParserPort4 => EmwAtmPres %d", port4.EmwAtmPres)
				index = index + 3
			}

		// 			// EM R102
		// 			// case 5:

		// 			// EM ACW100 & EM THW 100/200/201
		// 			// case 6:

		default:
			// fmt.Print("Data not parsed by decode.LoRaImt imtIotProtocolParser()\n")
			fmt.Print("Data not parsed default PORT4\n")
			// break PL

			// }
		}
	}
	p, err := json.Marshal(port4)
	if err != nil {
		fmt.Println(err)
		return "Port4 data parsed wrongly"
	}
	return string(p[:])
}

func protocolParserPort100(bytes []byte) string {
	var port100 Port100

	len := len(bytes)
	_0d := 0
	_0e := 0
	_03 := 0
	_01 := 0

PL: // Parse Loop
	for i := 0; i < len; i++ {
		switch bytes[i] {
		// case 0x00:
		// fmt.Println("00")

		case 0x01:
			switch _01 {
			case 0:
				v := uint64(bytes[i+1]) << 8
				v |= uint64(bytes[i+2])
				f := float64(v) / 10
				i = i + 2
				_01 = _01 + 1
				port100.X_01_0 = f

			case 1:
				v := uint64(bytes[i+1]) << 8
				v |= uint64(bytes[i+2])
				f := float64(v) / 10
				i = i + 2
				_01 = _01 + 1
				port100.X_01_1 = f

			case 2:
				v := uint64(bytes[i+1]) << 8
				v |= uint64(bytes[i+2])
				f := float64(v) / 10
				i = i + 2
				_01 = _01 + 1
				port100.X_01_2 = f

			case 3:
				v := uint64(bytes[i+1]) << 8
				v |= uint64(bytes[i+2])
				f := float64(v) / 10
				i = i + 2
				_01 = _01 + 1
				port100.X_01_3 = f

			case 4:
				v := uint64(bytes[i+1]) << 8
				v |= uint64(bytes[i+2])
				f := float64(v) / 10
				i = i + 2
				_01 = _01 + 1
				port100.X_01_4 = f

			case 5:
				v := uint64(bytes[i+1]) << 8
				v |= uint64(bytes[i+2])
				f := float64(v) / 10
				i = i + 2
				_01 = _01 + 1
				port100.X_01_5 = f

			case 6:
				v := uint64(bytes[i+1]) << 8
				v |= uint64(bytes[i+2])
				f := float64(v) / 10
				i = i + 2
				_01 = _01 + 1
				port100.X_01_6 = f

			case 7:
				v := uint64(bytes[i+1]) << 8
				v |= uint64(bytes[i+2])
				f := float64(v) / 10
				i = i + 2
				_01 = _01 + 1
				port100.X_01_7 = f
			}

		case 0x02:
			v := uint64(bytes[i+1]) << 8
			v |= uint64(bytes[i+2])
			f := float64(v) / 10
			i = i + 2
			port100.X_02 = f

		case 0x03:
			switch _03 {
			case 0:
				v := uint64(bytes[i+3]) << 8
				v |= uint64(bytes[i+4])
				f := float64(v)
				port100.X_03_0 = f
				i = i + 2
				_03 = _03 + 1
			case 1:
				v := uint64(bytes[i+3]) << 8
				v |= uint64(bytes[i+4])
				f := float64(v)
				port100.X_03_1 = f
				i = i + 2
				_03 = _03 + 1
			}

			// case 0x03:
			//   var press = {};
			//   press.v = (bytes[index++]<<8) | bytes[index++];
			//   press.n = "press";
			//   press.u = "hPa";
			//   decoded.modules.push(press);
			//   break;

			// 	// case 0x04:
			// 	//   var corrente = {};
			// 	//   corrente.v = (bytes[index++]<<8) | bytes[index++];
			// 	//   corrente.n = "corrente";
			// 	//   corrente.u = "A";
			// 	//   decoded.modules.push(corrente);
			// 	//   break;

		case 0x05:
			v := uint64(bytes[i+1]) << 8
			v |= uint64(bytes[i+2])
			f := float64(v) / 10
			port100.X_05_0 = f
			v = uint64(bytes[i+3]) << 8
			v |= uint64(bytes[i+4])
			f = float64(v) / 10
			port100.X_05_1 = f
			v = uint64(bytes[i+5]) << 8
			v |= uint64(bytes[i+6])
			f = float64(v) / 10
			port100.X_05_2 = f
			i = i + 6

			// 	// case 0x06:
			// 	//   var accx = {};
			// 	//   accx.v = (bytes[index++]<<8) | bytes[index++];
			// 	//   accx.n = "AceleromeroX";
			// 	//   accx.u = "g";
			// 	//   decoded.modules.push(accx);
			// 	//   var accy = {};
			// 	//   accy.v = (bytes[index++]<<8) | bytes[index++];
			// 	//   accy.n = "AceleromeroY";
			// 	//   accy.u = "g";
			// 	//   decoded.modules.push(accy);
			// 	//   var accz = {};
			// 	//   accz.v = (bytes[index++]<<8) | bytes[index++];
			// 	//   accz.n = "AceleromeroZ";
			// 	//   accz.u = "g";
			// 	//   decoded.modules.push(accz);
			// 	//   break;

			// 	// case 0x07:
			// 	//   var magx = {};
			// 	//   magx.v = (bytes[index++]<<8) | bytes[index++];
			// 	//   magx.n = "MagnetometroX";
			// 	//   magx.u = "mGauss";
			// 	//   decoded.modules.push(magx);
			// 	//   var magy = {};
			// 	//   magy.v = (bytes[index++]<<8) | bytes[index++];
			// 	//   magy.n = "MagnetometroY";
			// 	//   magy.u = "mGauss";
			// 	//   decoded.modules.push(magy);
			// 	//   var magz = {};
			// 	//   magz.v = (bytes[index++]<<8) | bytes[index++];
			// 	//   magz.n = "MagnetometroZ";
			// 	//   magz.u = "mGauss";
			// 	//   decoded.modules.push(magz);
			// 	//   break;

			// 	// case 0x08:
			// 	//     //data.rtc = data.remainingData.slice(0,6);
			// 	//     bytes[index++];bytes[index++];
			// 	//     bytes[index++];
			// 	//     bytes[index++];
			// 	//     break;

			// 	// case 0x09:
			// 	//     //data.date = data.remainingData.slice(0,8);
			// 	//     bytes[index++];bytes[index++];
			// 	//     bytes[index++];bytes[index++];

			// 	//     break;

		case 0x0A:
			var f float64
			v := uint64(bytes[i+1])
			a := uint64(bytes[i+2]) << 16
			a |= uint64(bytes[i+3]) << 8
			a |= uint64(bytes[i+4])
			b := float64(a) / 1000000

			if v > 127 {
				f = -((255 - float64(v)) + 1) - b //complement of 2
			} else {
				f = float64(v) + b
			}
			port100.X_0A_0 = f

			v = uint64(bytes[i+5])
			a = uint64(bytes[i+6]) << 16
			a |= uint64(bytes[i+7]) << 8
			a |= uint64(bytes[i+8])
			b = float64(a) / 1000000

			if v > 127 {
				f = -((255 - float64(v)) + 1) - b //complement of 2
			} else {
				f = float64(v) + b
			}
			port100.X_0A_1 = f
			i = i + 8

		case 0x0B:
			v := uint64(bytes[i+1]) << 16
			v |= uint64(bytes[i+2]) << 8
			v |= uint64(bytes[i+3])
			port100.X_0B = v
			i = i + 3

		case 0x0C:
			v := uint64(bytes[i+1]) << 8
			v |= uint64(bytes[i+2])
			f := float64(v) / 1000
			port100.X_0C = f
			i = i + 2

		case 0x0D:
			switch _0d {
			case 0:
				v := uint64(bytes[i+1]) << 8
				v |= uint64(bytes[i+2])
				port100.X_0D_0 = v
				i = i + 2
				_0d = _0d + 1

			case 1:
				v := uint64(bytes[i+1]) << 8
				v |= uint64(bytes[i+2])
				port100.X_0D_1 = v
				i = i + 2
				_0d = _0d + 1

			case 2:
				v := uint64(bytes[i+1]) << 8
				v |= uint64(bytes[i+2])
				port100.X_0D_2 = v
				i = i + 2
				_0d = _0d + 1

			case 3:
				v := uint64(bytes[i+1]) << 8
				v |= uint64(bytes[i+2])
				port100.X_0D_3 = v
				i = i + 2
				_0d = _0d + 1
			}

		case 0x0E:
			switch _0e {
			case 0:
				v := uint64(bytes[i+1]) << 24
				v |= uint64(bytes[i+2]) << 16
				v |= uint64(bytes[i+3]) << 8
				v |= uint64(bytes[i+4])
				f := float64(v) * (150 / 5) / 2000
				port100.X_0E_0 = f
				i = i + 4
				_0e = _0e + 1

			case 1:
				v := uint64(bytes[i+1]) << 24
				v |= uint64(bytes[i+2]) << 16
				v |= uint64(bytes[i+3]) << 8
				v |= uint64(bytes[i+4])
				f := float64(v) * (150 / 5) / 2000
				port100.X_0E_1 = f
				i = i + 4
				_0e = _0e + 1
			}
			// 	// case 0x0F:
			// 	//     //data.rfid = data.remainingData.slice(0,16);
			// 	//     bytes[index++];bytes[index++];
			// 	//     bytes[index++];bytes[index++];
			// 	//     bytes[index++];bytes[index++];
			// 	//     bytes[index++];bytes[index++];
			// 	//     break;

		case 0x10:
			v := uint64(bytes[i+1]) << 8
			v |= uint64(bytes[i+2])
			port100.X_10 = v
			i = i + 2

		case 0x11:
			v := uint64(bytes[i+1]) << 8
			v |= uint64(bytes[i+2])
			f := float64(v) / 100
			port100.X_11 = f
			i = i + 2

			// 	// case 0x12:
			// 	//     //data.color = data.remainingData.slice(0,4);
			// 	//     bytes[index++];bytes[index++];
			// 	//     break;

		case 0x13:
			v := uint64(bytes[i+1]) << 8
			v |= uint64(bytes[i+2])
			port100.X_13 = v
			i = i + 2

		// 	// case 0x14:
		// 	//     //data.heartbeat = data.remainingData.slice(0,4);
		// 	//     bytes[index++];bytes[index++];
		// 	//     break;

		// 	// case 0x15:
		// 	//     //data.oxigenVolume = data.remainingData.slice(0,4);
		// 	//     bytes[index++];bytes[index++];
		// 	//     break;

		// case 0x16:
		// 	// NEED TO DEBUG
		// 	for j := 0; j < 17; j++ {
		// 		sb.WriteString("data_fft_")
		// 		sb.WriteString(strconv.FormatUint(uint64(j), 10))
		// 		sb.WriteString("=")
		// 		v := uint64(bytes[i+1])
		// 		sb.WriteString(strconv.FormatUint(v, 10))
		// 		sb.WriteString(",")
		// 	}
		// 	i = i + 17

		default:
			// fmt.Print("Data not parsed by decode.LoRaImt imtIotProtocolParser()\n")
			break PL
		}
	}

	p, err := json.Marshal(port100)
	if err != nil {
		fmt.Println(err)
		return "Port100 data parsed wrongly"
	}
	return string(p[:])
}

// CONVERT B64 to BYTE
func b64ToByte(b64 string) ([]byte, error) {
	b, err := base64.StdEncoding.DecodeString(b64)
	if err != nil {
		log.Fatal(err)
	}
	return b, err
}

// func parseLnsMeasurement(measurement string, data string, port uint64) string {
func parseLnsMeasurement(schema Schema, data string, port uint64) string {
	// measurements format
	// TODO:
	var sb strings.Builder

	// var dev map[string]interface{}
	// err := json.Unmarshal([]byte(schema), &dev)

	if data == "" {
		return "No data"
	}

	// B64 to Byte
	b, err := b64ToByte(data)
	if err != nil {
		// fmt.Print(data)
		log.Panic(err)
	}

	// TODO: SELECT PORT -> DECODE DATA ACCORDING PORT -> SELECT MEASUREMENT -> RETURN STRING
	switch port {
	case 100:
		var port100 Port100
		d := protocolParserPort100(b)
		json.Unmarshal([]byte(d), &port100)

		fields := schema.Fields
		switch schema.Name {
		case "SmartLight":
			var smartLight SmartLight
			temperature := fields["temperature"].(map[string]interface{})
			humidity := fields["humidity"].(map[string]interface{})
			movement := fields["movement"].(map[string]interface{})
			luminosity := fields["luminosity"].(map[string]interface{})
			batteryVoltage := fields["batteryVoltage"].(map[string]interface{})
			boardVoltage := fields["boardVoltage"].(map[string]interface{})

			fmt.Printf("\nmeasurement %s", schema.Fields["temperature"])
			smartLight.Temperature = port100.X_01_0*temperature["scale"].(float64) + temperature["offset"].(float64)
			smartLight.Humidity = port100.X_02*humidity["scale"].(float64) + humidity["offset"].(float64)
			smartLight.Movement = uint64(float64(port100.X_0B)*movement["scale"].(float64) + movement["offset"].(float64))
			smartLight.Luminosity = float64(port100.X_0D_0)*luminosity["scale"].(float64) + luminosity["offset"].(float64)
			smartLight.BatteryVoltage = float64(port100.X_0D_1)*batteryVoltage["scale"].(float64) + batteryVoltage["offset"].(float64)
			smartLight.BoardVoltage = port100.X_0C*boardVoltage["scale"].(float64) + boardVoltage["offset"].(float64)

			// sb.WriteString(`,temperature=`)
			// sb.WriteString(strconv.FormatFloat(smartLight.Temperature, 'f', -1, 64))
			// sb.WriteString(`,humidity=`)
			// sb.WriteString(strconv.FormatFloat(smartLight.Humidity, 'f', -1, 64))
			// sb.WriteString(`,movement=`)
			// sb.WriteString(strconv.FormatUint(uint64(smartLight.Movement), 10))
			// sb.WriteString(`,luminosity=`)
			// sb.WriteString(strconv.FormatFloat(smartLight.Luminosity, 'f', -1, 64))
			// sb.WriteString(`,batteryVoltage=`)
			// sb.WriteString(strconv.FormatFloat(smartLight.BatteryVoltage, 'f', -1, 64))
			// sb.WriteString(`,boardVoltage=`)
			// sb.WriteString(strconv.FormatFloat(smartLight.BoardVoltage, 'f', -1, 64))

		case "WaterTankLevel":
			var waterTankLevel WaterTankLevel
			waterTankLevel.Distance = port100.X_13
			waterTankLevel.BoardVoltage = port100.X_0C

			sb.WriteString(`,distance=`)
			sb.WriteString(strconv.FormatUint(uint64(waterTankLevel.Distance), 10))
			sb.WriteString(`,boardVoltage=`)
			sb.WriteString(strconv.FormatFloat(waterTankLevel.BoardVoltage, 'f', -1, 64))

		case "GaugePressure":
			var gaugePressure GaugePressure
			gaugePressure.InletPressure = float64(port100.X_0D_0)
			gaugePressure.OutletPressure = float64(port100.X_0D_1)
			gaugePressure.BoardVoltage = port100.X_0C

			sb.WriteString(`,inletPressure=`)
			sb.WriteString(strconv.FormatFloat(gaugePressure.InletPressure, 'f', -1, 64))
			sb.WriteString(`,outletPressure=`)
			sb.WriteString(strconv.FormatFloat(gaugePressure.OutletPressure, 'f', -1, 64))
			sb.WriteString(`,boardVoltage=`)
			sb.WriteString(strconv.FormatFloat(gaugePressure.BoardVoltage, 'f', -1, 64))

		case "Hydrometer":
			var hydrometer Hydrometer
			hydrometer.Counter = port100.X_0B
			hydrometer.BoardVoltage = port100.X_0C

			sb.WriteString(`,counter=`)
			sb.WriteString(strconv.FormatUint(uint64(hydrometer.Counter), 10))
			sb.WriteString(`,boardVoltage=`)
			sb.WriteString(strconv.FormatFloat(hydrometer.BoardVoltage, 'f', -1, 64))

		case "EnergyMeter":
			var energyMeter EnergyMeter
			energyMeter.ForwardEnergy = port100.X_0E_0
			energyMeter.ReverseEnergy = port100.X_0E_1
			energyMeter.BoardVoltage = port100.X_0C

			sb.WriteString(`,forwardEnergy=`)
			sb.WriteString(strconv.FormatFloat(energyMeter.ForwardEnergy, 'f', -1, 64))
			sb.WriteString(`,reverseEnergy=`)
			sb.WriteString(strconv.FormatFloat(energyMeter.ReverseEnergy, 'f', -1, 64))
			sb.WriteString(`,boardVoltage=`)
			sb.WriteString(strconv.FormatFloat(energyMeter.BoardVoltage, 'f', -1, 64))

		case "Sprinkler":
			var sprinkler Sprinkler
			solenoid1 := port100.X_0D_0
			solenoid2 := port100.X_0D_1
			solenoid3 := port100.X_0D_2

			if solenoid1 > 1500 {
				sprinkler.Solenoid1 = true
			} else {
				sprinkler.Solenoid1 = false
			}

			if solenoid2 > 1500 {
				sprinkler.Solenoid2 = true
			} else {
				sprinkler.Solenoid2 = false
			}

			if solenoid3 > 1500 {
				sprinkler.Solenoid3 = true
			} else {
				sprinkler.Solenoid3 = false
			}

			sprinkler.Counter = port100.X_0B
			sprinkler.BoardVoltage = port100.X_0C

			sb.WriteString(`,solenoid1=`)
			sb.WriteString(strconv.FormatBool(sprinkler.Solenoid1))
			sb.WriteString(`,solenoid2=`)
			sb.WriteString(strconv.FormatBool(sprinkler.Solenoid2))
			sb.WriteString(`,solenoid3=`)
			sb.WriteString(strconv.FormatBool(sprinkler.Solenoid3))
			sb.WriteString(`,counter=`)
			sb.WriteString(strconv.FormatUint(uint64(sprinkler.Counter), 10))
			sb.WriteString(`,boardVoltage=`)
			sb.WriteString(strconv.FormatFloat(sprinkler.BoardVoltage, 'f', -1, 64))

		case "SoilMoisture3DepthLevels":
			var soilMoisture3DepthLevels SoilMoisture3DepthLevels
			soilMoisture3DepthLevels.SoilMoistureDepthLevel1 = port100.X_0D_2
			soilMoisture3DepthLevels.SoilMoistureDepthLevel2 = port100.X_0D_1
			soilMoisture3DepthLevels.SoilMoistureDepthLevel3 = port100.X_0D_0
			soilMoisture3DepthLevels.BoardVoltage = port100.X_0C

			sb.WriteString(`,soilMoistureDepthLevel1=`)
			sb.WriteString(strconv.FormatUint(uint64(soilMoisture3DepthLevels.SoilMoistureDepthLevel1), 10))
			sb.WriteString(`,soilMoistureDepthLevel2=`)
			sb.WriteString(strconv.FormatUint(uint64(soilMoisture3DepthLevels.SoilMoistureDepthLevel2), 10))
			sb.WriteString(`,soilMoistureDepthLevel3=`)
			sb.WriteString(strconv.FormatUint(uint64(soilMoisture3DepthLevels.SoilMoistureDepthLevel3), 10))
			sb.WriteString(`,boardVoltage=`)
			sb.WriteString(strconv.FormatFloat(soilMoisture3DepthLevels.BoardVoltage, 'f', -1, 64))

		case "MilkFat":
			// var smartLight SmartLight
			// smartLight.Temperature = port100.X_01_0
			// smartLight.Humidity = port100.X_02
			// smartLight.Movement = port100.X_0B
			// smartLight.Luminosity = float64(port100.X_0D_0)
			// smartLight.BatteryVoltage = float64(port100.X_0D_1)
			// smartLight.BoardVoltage = port100.X_0C

			// sb.WriteString(`,temperature=`)
			// sb.WriteString(strconv.FormatFloat(smartLight.Temperature, 'f', -1, 64))
			// sb.WriteString(`,humidity=`)
			// sb.WriteString(strconv.FormatFloat(smartLight.Humidity, 'f', -1, 64))
			// sb.WriteString(`,movement=`)
			// sb.WriteString(strconv.FormatUint(uint64(smartLight.Movement), 10))
			// sb.WriteString(`,luminosity=`)
			// sb.WriteString(strconv.FormatFloat(smartLight.Luminosity, 'f', -1, 64))
			// sb.WriteString(`,batteryVoltage=`)
			// sb.WriteString(strconv.FormatFloat(smartLight.BatteryVoltage, 'f', -1, 64))
			// sb.WriteString(`,boardVoltage=`)
			// sb.WriteString(strconv.FormatFloat(smartLight.BoardVoltage, 'f', -1, 64))

		case "GPS":
			// var smartLight SmartLight
			// smartLight.Temperature = port100.X_01_0
			// smartLight.Humidity = port100.X_02
			// smartLight.Movement = port100.X_0B
			// smartLight.Luminosity = float64(port100.X_0D_0)
			// smartLight.BatteryVoltage = float64(port100.X_0D_1)
			// smartLight.BoardVoltage = port100.X_0C

			// sb.WriteString(`,temperature=`)
			// sb.WriteString(strconv.FormatFloat(smartLight.Temperature, 'f', -1, 64))
			// sb.WriteString(`,humidity=`)
			// sb.WriteString(strconv.FormatFloat(smartLight.Humidity, 'f', -1, 64))
			// sb.WriteString(`,movement=`)
			// sb.WriteString(strconv.FormatUint(uint64(smartLight.Movement), 10))
			// sb.WriteString(`,luminosity=`)
			// sb.WriteString(strconv.FormatFloat(smartLight.Luminosity, 'f', -1, 64))
			// sb.WriteString(`,batteryVoltage=`)
			// sb.WriteString(strconv.FormatFloat(smartLight.BatteryVoltage, 'f', -1, 64))
			// sb.WriteString(`,boardVoltage=`)
			// sb.WriteString(strconv.FormatFloat(smartLight.BoardVoltage, 'f', -1, 64))

		case "Temperature8Point":
			var temperature8Point Temperature8Point
			auxTemperature1 := uint64(port100.X_01_2 * 10)
			auxTemperature2 := uint64(port100.X_01_3 * 10)
			auxTemperature3 := uint64(port100.X_01_4 * 10)
			auxTemperature4 := uint64(port100.X_01_5 * 10)
			auxTemperature5 := uint64(port100.X_01_6 * 10)
			auxTemperature6 := uint64(port100.X_01_7 * 10)
			auxTemperature7 := uint64(port100.X_01_1 * 10)
			auxTemperature8 := uint64(port100.X_01_0 * 10)

			if auxTemperature1&0x8000 > 0 {
				auxTemperature1 = auxTemperature1 - 0x10000
			}
			temperature8Point.Temperature1 = float64(auxTemperature1+3) / 10

			if auxTemperature2&0x8000 > 0 {
				auxTemperature2 = auxTemperature2 - 0x10000
			}
			temperature8Point.Temperature2 = float64(auxTemperature2+7) / 10

			if auxTemperature3&0x8000 > 0 {
				auxTemperature3 = auxTemperature3 - 0x10000
			}
			temperature8Point.Temperature3 = float64(auxTemperature3+3) / 10

			if auxTemperature4&0x8000 > 0 {
				auxTemperature4 = auxTemperature4 - 0x10000
			}
			temperature8Point.Temperature4 = float64(auxTemperature4+4) / 10

			if auxTemperature5&0x8000 > 0 {
				auxTemperature5 = auxTemperature5 - 0x10000
			}
			temperature8Point.Temperature5 = float64(auxTemperature5+6) / 10

			if auxTemperature6&0x8000 > 0 {
				auxTemperature6 = auxTemperature6 - 0x10000
			}
			temperature8Point.Temperature6 = float64(auxTemperature6+6) / 10

			if auxTemperature7&0x8000 > 0 {
				auxTemperature7 = auxTemperature7 - 0x10000
			}
			temperature8Point.Temperature7 = float64(auxTemperature7+7) / 10

			if auxTemperature8&0x8000 > 0 {
				auxTemperature8 = auxTemperature8 - 0x10000
			}
			temperature8Point.Temperature8 = float64(auxTemperature8-18) / 10

			temperature8Point.BoardVoltage = port100.X_0C

			sb.WriteString(`,temperature1=`)
			sb.WriteString(strconv.FormatFloat(temperature8Point.Temperature1, 'f', -1, 64))
			sb.WriteString(`,temperature2=`)
			sb.WriteString(strconv.FormatFloat(temperature8Point.Temperature2, 'f', -1, 64))
			sb.WriteString(`,temperature3=`)
			sb.WriteString(strconv.FormatFloat(temperature8Point.Temperature3, 'f', -1, 64))
			sb.WriteString(`,temperature4=`)
			sb.WriteString(strconv.FormatFloat(temperature8Point.Temperature4, 'f', -1, 64))
			sb.WriteString(`,temperature5=`)
			sb.WriteString(strconv.FormatFloat(temperature8Point.Temperature5, 'f', -1, 64))
			sb.WriteString(`,temperature6=`)
			sb.WriteString(strconv.FormatFloat(temperature8Point.Temperature6, 'f', -1, 64))
			sb.WriteString(`,temperature7=`)
			sb.WriteString(strconv.FormatFloat(temperature8Point.Temperature7, 'f', -1, 64))
			sb.WriteString(`,temperature8=`)
			sb.WriteString(strconv.FormatFloat(temperature8Point.Temperature8, 'f', -1, 64))
			sb.WriteString(`,boardVoltage=`)
			sb.WriteString(strconv.FormatFloat(temperature8Point.BoardVoltage, 'f', -1, 64))

		case "VibrationAverage":
			var vibrationAverage VibrationAverage
			vibrationAverage.Temperature = port100.X_01_0
			vibrationAverage.Humidity = port100.X_02
			vibrationAverage.VibrationAverageX = port100.X_05_0
			vibrationAverage.VibrationAverageY = port100.X_05_1
			vibrationAverage.VibrationAverageZ = port100.X_05_2
			vibrationAverage.BoardVoltage = port100.X_0C

			sb.WriteString(`,temperature=`)
			sb.WriteString(strconv.FormatFloat(vibrationAverage.Temperature, 'f', -1, 64))
			sb.WriteString(`,humidity=`)
			sb.WriteString(strconv.FormatFloat(vibrationAverage.Humidity, 'f', -1, 64))
			sb.WriteString(`,vibrationAverageX=`)
			sb.WriteString(strconv.FormatFloat(vibrationAverage.VibrationAverageX, 'f', -1, 64))
			sb.WriteString(`,vibrationAverageY=`)
			sb.WriteString(strconv.FormatFloat(vibrationAverage.VibrationAverageY, 'f', -1, 64))
			sb.WriteString(`,vibrationAverageZ=`)
			sb.WriteString(strconv.FormatFloat(vibrationAverage.VibrationAverageZ, 'f', -1, 64))
			sb.WriteString(`,boardVoltage=`)
			sb.WriteString(strconv.FormatFloat(vibrationAverage.BoardVoltage, 'f', -1, 64))
		default:
		}

	case 4:
		var port4 Port4
		d := protocolParserPort4(b)
		json.Unmarshal([]byte(d), &port4)

		switch schema.Name {
		case "WeatherStation":
			var weatherStation WeatherStation

			weatherStation.PowerSource = port4.PowerSource
			sb.WriteString(`,powerSource=`)
			sb.WriteString(strconv.FormatBool(weatherStation.PowerSource))

			if port4.IsEnvSensorFailStatus == true {
				weatherStation.EnvSensorFailStatus = port4.EnvSensorFailStatus
				sb.WriteString(`,envSensorFailStatus=`)
				sb.WriteString(strconv.FormatBool(weatherStation.EnvSensorFailStatus))
			}
			if port4.IsInternalBatteryVoltage == true {
				weatherStation.InternalBatteryVoltage = port4.InternalBatteryVoltage
				sb.WriteString(`,internalBatteryVoltage=`)
				sb.WriteString(strconv.FormatFloat(weatherStation.InternalBatteryVoltage, 'f', -1, 64))
			}
			if port4.IsFirmwareVersion == true {
				weatherStation.FirmwareVersion = port4.FirmwareVersion
				sb.WriteString(`,firmwareVersion=`)
				sb.WriteString(strconv.FormatUint(weatherStation.FirmwareVersion, 10))
			}
			if port4.IsC1State == true {
				weatherStation.C1State = port4.C1State
				sb.WriteString(`,c1State=`)
				sb.WriteString(strconv.FormatBool(weatherStation.C1State))
			}
			if port4.IsC1Count == true {
				weatherStation.C1Count = port4.C1Count
				sb.WriteString(`,c1Count=`)
				sb.WriteString(strconv.FormatUint(weatherStation.C1Count, 10))
			}
			if port4.IsC2State == true {
				weatherStation.C2State = port4.C2State
				sb.WriteString(`,c2State=`)
				sb.WriteString(strconv.FormatBool(weatherStation.C2State))
			}
			if port4.IsC2Count == true {
				weatherStation.C2Count = port4.C2Count
				sb.WriteString(`,c2Count=`)
				sb.WriteString(strconv.FormatUint(weatherStation.C2Count, 10))
			}
			if port4.IsInternalTemperature == true {
				weatherStation.InternalTemperature = port4.InternalTemperature
				sb.WriteString(`,internalTemperature=`)
				sb.WriteString(strconv.FormatFloat(weatherStation.InternalTemperature, 'f', -1, 64))
			}
			if port4.IsInternalHumidity == true {
				weatherStation.InternalHumidity = port4.InternalHumidity
				sb.WriteString(`,internalHumidity=`)
				sb.WriteString(strconv.FormatFloat(weatherStation.InternalHumidity, 'f', -1, 64))
			}
			if port4.IsEmwRainLevel == true {
				weatherStation.EmwRainLevel = port4.EmwRainLevel
				sb.WriteString(`,emwRainLevel=`)
				sb.WriteString(strconv.FormatFloat(weatherStation.EmwRainLevel, 'f', -1, 64))
			}
			if port4.IsEmwAvgWindSpeed == true {
				weatherStation.EmwAvgWindSpeed = port4.EmwAvgWindSpeed
				sb.WriteString(`,emwAvgWindSpeed=`)
				sb.WriteString(strconv.FormatUint(weatherStation.EmwAvgWindSpeed, 10))
			}
			if port4.IsEmwGustWindSpeed == true {
				weatherStation.EmwGustWindSpeed = port4.EmwGustWindSpeed
				sb.WriteString(`,emwGustWindSpeed=`)
				sb.WriteString(strconv.FormatUint(weatherStation.EmwGustWindSpeed, 10))
			}
			if port4.IsEmwWindDirection == true {
				weatherStation.EmwWindDirection = port4.EmwWindDirection
				sb.WriteString(`,emwWindDirection=`)
				sb.WriteString(strconv.FormatUint(weatherStation.EmwWindDirection, 10))
			}
			if port4.IsEmwTemperature == true {
				weatherStation.EmwTemperature = port4.EmwTemperature
				sb.WriteString(`,emwTemperature=`)
				sb.WriteString(strconv.FormatFloat(weatherStation.EmwTemperature, 'f', -1, 64))
			}
			if port4.IsEmwHumidity == true {
				weatherStation.EmwHumidity = port4.EmwHumidity
				sb.WriteString(`,emwHumidity=`)
				sb.WriteString(strconv.FormatUint(weatherStation.EmwHumidity, 10))
			}
			if port4.IsEmwLuminosity == true {
				weatherStation.EmwLuminosity = port4.EmwLuminosity
				sb.WriteString(`,emwLuminosity=`)
				sb.WriteString(strconv.FormatUint(weatherStation.EmwLuminosity, 10))
			}
			if port4.IsEmwUv == true {
				weatherStation.EmwUv = port4.EmwUv
				sb.WriteString(`,emwUv=`)
				sb.WriteString(strconv.FormatFloat(weatherStation.EmwUv, 'f', -1, 64))
			}
			if port4.IsEmwSolarRadiation == true {
				weatherStation.EmwSolarRadiation = port4.EmwSolarRadiation
				sb.WriteString(`,emwSolarRadiation=`)
				sb.WriteString(strconv.FormatFloat(weatherStation.EmwSolarRadiation, 'f', -1, 64))
			}
			if port4.IsEmwAtmPres == true {
				weatherStation.EmwAtmPres = port4.EmwAtmPres
				sb.WriteString(`,emwAtmPres=`)
				sb.WriteString(strconv.FormatFloat(weatherStation.EmwAtmPres, 'f', -1, 64))
			}
		default:
		}
	}
	return sb.String()
}

// func parseLns(measurement string, deviceId string, direction string, etc string, message string) string {
func parseLns(schema Schema, direction string, etc string, message string) string {
	// measurement, deviceId, direction, etc, incoming[1]
	var sb strings.Builder
	var lnsUp LnsUp
	var lnsCommand LnsCommand
	var lnsImtUp LnsImtUp
	var alert Alert
	// var lnsImtCommand LnsImtCommand
	var lnsChirpStackV4Up LnsChirpStackV4Up
	// var lnsChirpstackV4Command LnsChirpstackV4Command

	// fmt.Printf("\nmeasurement %s", measurement)
	// fmt.Printf("\ndeviceId %s", deviceId)
	// fmt.Printf("\ndirection %s", direction)
	// fmt.Printf("\netc %s", etc)
	// fmt.Printf("\nmessage %s", message)

	if message == "" {
		// return message, errors.New("empty message to parse")
		// return influx
		return "No message to parse"
	}

	switch etc {
	case "imt":
		if direction == "up" {
			fmt.Printf("\nBefore Unmarshal %v", schema)
			fmt.Printf("\nMessage %v", message)

			json.Unmarshal([]byte(message), &lnsImtUp)
			fmt.Printf("\nAfter Unmarshal")

			lnsUp.Measurement = schema.Name
			lnsUp.DeviceId = lnsImtUp.DevEUI
			lnsUp.RxInfoMac_0 = lnsImtUp.RxInfo[0].Mac
			lnsUp.RxInfoTime_0 = lnsImtUp.RxInfo[0].Time.Unix() * 1000 * 1000 * 1000
			lnsUp.RxInfoRssi_0 = lnsImtUp.RxInfo[0].Rssi
			lnsUp.RxInfoSnr_0 = lnsImtUp.RxInfo[0].LoRaSNR
			lnsUp.RxInfoLat_0 = lnsImtUp.RxInfo[0].Latitude
			lnsUp.RxInfoLon_0 = lnsImtUp.RxInfo[0].Longitude
			lnsUp.RxInfoAlt_0 = lnsImtUp.RxInfo[0].Altitude
			lnsUp.TxInfoFrequency = lnsImtUp.TxInfo.Frequency / 1000000
			lnsUp.TxInfoModulation = lnsImtUp.TxInfo.DataRate.Modulation
			lnsUp.TxInfoBandWidth = lnsImtUp.TxInfo.DataRate.Bandwidth
			lnsUp.TxInfoSpreadFactor = lnsImtUp.TxInfo.DataRate.SpreadFactor
			// lnsUp.TxInfoCodeRate = lnsImtUp.TxInfo.CodeRate
			lnsUp.FCnt = lnsImtUp.FCnt
			lnsUp.FPort = lnsImtUp.FPort
			lnsUp.FType = "uplink"
			lnsUp.Data = lnsImtUp.Data
		}

	case "chirpstackv4":
		if direction == "up" {
			json.Unmarshal([]byte(message), &lnsChirpStackV4Up)
			// fmt.Printf("\nmessage from chirpstackv4 parseLns %s", message)

			lnsUp.Measurement = schema.Name
			lnsUp.DeviceId = lnsChirpStackV4Up.DeviceInfo.DevEui
			lnsUp.RxInfoMac_0 = lnsChirpStackV4Up.RxInfo[0].GatewayId
			lnsUp.RxInfoTime_0 = lnsChirpStackV4Up.RxInfo[0].NsTime.UnixNano()
			lnsUp.RxInfoRssi_0 = lnsChirpStackV4Up.RxInfo[0].Rssi
			lnsUp.RxInfoSnr_0 = lnsChirpStackV4Up.RxInfo[0].Snr
			lnsUp.RxInfoLat_0 = lnsChirpStackV4Up.RxInfo[0].Location.Latitude
			lnsUp.RxInfoLon_0 = lnsChirpStackV4Up.RxInfo[0].Location.Longitude
			lnsUp.RxInfoAlt_0 = lnsChirpStackV4Up.RxInfo[0].Location.Altitude
			lnsUp.TxInfoFrequency = lnsChirpStackV4Up.TxInfo.Frequency / 1000000
			lnsUp.TxInfoModulation = "LORA"
			lnsUp.TxInfoBandWidth = lnsChirpStackV4Up.TxInfo.Modulation.Lora.Bandwidth / 1000
			lnsUp.TxInfoSpreadFactor = lnsChirpStackV4Up.TxInfo.Modulation.Lora.SpreadingFactor
			// lnsUp.TxInfoCodeRate = lnsChirpStackV4Up.TxInfo.Modulation.Lora.CodeRate
			lnsUp.FCnt = lnsChirpStackV4Up.FCnt
			lnsUp.FPort = lnsChirpStackV4Up.FPort
			lnsUp.FType = "uplink"
			lnsUp.Data = lnsChirpStackV4Up.Data
			// fmt.Printf("\nlnsUp.Data %s", lnsUp.Data)
		}

	case "atc":
		// lns.Measurement = measurement
		// lns.DeviceId = lnsImt.DevEUI
		// lns.RxInfoMac_0 = lnsImt.RxInfo[0].Mac
		// lns.RxInfoTime_0 = lnsImt.RxInfo[0].Time.Unix() * 1000 * 1000 * 1000
		// lns.RxInfoRssi_0 = lnsImt.RxInfo[0].Rssi
		// lns.RxInfoSnr_0 = lnsImt.RxInfo[0].LoRaSNR
		// lns.RxInfoLat_0 = lnsImt.RxInfo[0].Latitude
		// lns.RxInfoLon_0 = lnsImt.RxInfo[0].Longitude
		// lns.RxInfoAlt_0 = lnsImt.RxInfo[0].Altitude
		// lns.TxInfoFrequency = lnsImt.TxInfo.Frequency / 1000000
		// lns.TxInfoModulation = lnsImt.TxInfo.DataRate.Modulation
		// lns.TxInfoBandWidth = lnsImt.TxInfo.DataRate.Bandwidth
		// lns.TxInfoSpreadFactor = lnsImt.TxInfo.DataRate.SpreadFactor
		// lns.TxInfoCodeRate = lnsImt.TxInfo.CodeRate
		// lns.FCnt = lnsImt.FCnt
		// lns.FPort = lnsImt.FPort
		// lns.FType = "uplink"
		// lns.Data = lnsImt.Data

	default:
	}

	// json.Unmarshal([]byte(message), &lns)

	if direction == "up" {
		// Measurement
		sb.WriteString(lnsUp.Measurement)
		// sb.WriteString(schema.name)

		// TODO: READ AGGREGATION-SCHEMA.JSON ACCORDING
		// ORGANIZATION.DEVICETYPE.MEASUREMENT.DEVICEID
		// THEN, CHECK FOR AGGREATIONS TAGS AND TRANSFORMATION FIELDS

		// Tags
		sb.WriteString(`,deviceType=LNS`)
		// sb.WriteString(`,deviceType=`)
		// sb.WriteString(`,schema.deviceType`)
		sb.WriteString(`,deviceId=`)
		sb.WriteString(schema.DeviceId)
		sb.WriteString(`,direction=`)
		sb.WriteString(direction)
		sb.WriteString(`,origin=`)
		sb.WriteString(etc)

		// sb.WriteString(`,type=`)
		// sb.WriteString(lnsUp.FType)
		sb.WriteString(`,rxMac_0=`)
		sb.WriteString(lnsUp.RxInfoMac_0)
		sb.WriteString(`,txModulation=`)
		sb.WriteString(lnsUp.TxInfoModulation)
		// sb.WriteString(`,txCodeRate=`)
		// sb.WriteString(lns.TxInfoCodeRate)

		// sb.WriteString(aggregateSchema(schema) -> schema.tags in sb.WriteString()
		sb.WriteString(`,deviceName=`)
		sb.WriteString(schema.Tags.DeviceName)
		sb.WriteString(`,deviceLocation=`)
		sb.WriteString(schema.Tags.DeviceLocation)
		sb.WriteString(`,organization=`)
		sb.WriteString(schema.Tags.Organization)

		// Fields
		sb.WriteString(` `)
		sb.WriteString(`,data="`)
		sb.WriteString(lnsUp.Data)
		sb.WriteString(`"`)
		sb.WriteString(parseLnsMeasurement(schema, lnsUp.Data, lnsUp.FPort))

		sb.WriteString(`,txFrequency=`)
		sb.WriteString(strconv.FormatFloat(lnsUp.TxInfoFrequency, 'f', -1, 64))
		sb.WriteString(`,txBandWidth=`)
		sb.WriteString(strconv.FormatUint(uint64(lnsUp.TxInfoBandWidth), 10))
		sb.WriteString(`,txSpreadFactor=`)
		sb.WriteString(strconv.FormatUint(uint64(lnsUp.TxInfoSpreadFactor), 10))
		sb.WriteString(`,rxRssi_0=`)
		sb.WriteString(strconv.FormatInt(int64(lnsUp.RxInfoRssi_0), 10))
		sb.WriteString(`,rxSnr_0=`)
		sb.WriteString(strconv.FormatFloat(lnsUp.RxInfoSnr_0, 'f', -1, 64))
		sb.WriteString(`,rxLat_0=`)
		sb.WriteString(strconv.FormatFloat(lnsUp.RxInfoLat_0, 'f', -1, 64))
		sb.WriteString(`,rxLon_0=`)
		sb.WriteString(strconv.FormatFloat(lnsUp.RxInfoLon_0, 'f', -1, 64))
		sb.WriteString(`,rxAlt_0=`)
		sb.WriteString(strconv.FormatUint(uint64(lnsUp.RxInfoAlt_0), 10))
		sb.WriteString(`,fPort=`)
		sb.WriteString(strconv.FormatUint(uint64(lnsUp.FPort), 10))
		sb.WriteString(`,fCnt=`)
		sb.WriteString(strconv.FormatUint(uint64(lnsUp.FCnt), 10))
		// sb.WriteString(`,data="`)
		// sb.WriteString(lnsUp.Data)
		// sb.WriteString(`"`)

		// sb.WriteString(parseLnsMeasurement(lnsUp.Measurement, lnsUp.Data, lnsUp.FPort))

		// Timestamp_ms
		sb.WriteString(` `)
		sb.WriteString(strconv.FormatInt(int64(lnsUp.RxInfoTime_0), 10))
	}
	// fmt.Printf("\n\nChirpstack %s\n\n", sb.String())

	if direction == "down" {
		json.Unmarshal([]byte(message), &lnsCommand)

		// Measurement
		// sb.WriteString("Lns")
		sb.WriteString(schema.Name)

		// Tags
		sb.WriteString(`,deviceType=LNS`)
		sb.WriteString(`,deviceId=`)
		sb.WriteString(schema.DeviceId)
		// sb.WriteString(`,type=downlink`)
		sb.WriteString(`,direction=`)
		sb.WriteString(direction)
		sb.WriteString(`,origin=`)
		sb.WriteString(etc)

		sb.WriteString(`,application=`)
		sb.WriteString(lnsCommand.Application)
		sb.WriteString(`,reference=`)
		sb.WriteString(lnsCommand.Reference)

		// Fields
		sb.WriteString(` `)
		sb.WriteString(`confirmed=`)
		sb.WriteString(strconv.FormatBool(lnsCommand.Confirmed))
		sb.WriteString(`,fPort=`)
		sb.WriteString(strconv.FormatUint(uint64(lnsCommand.FPort), 10))
		sb.WriteString(`,data="`)
		sb.WriteString(lnsCommand.Data)
		sb.WriteString(`"`)

		// Timestamp_ms
		sb.WriteString(` `)
		sb.WriteString(strconv.FormatInt(int64(lnsCommand.Timestamp), 10))
	}

	if direction == "alert" {
		json.Unmarshal([]byte(message), &alert)

		var trigger string
		var triggerAt string
		var triggerType string
		var actionSensor string

		if alert.Trigger != "" {
			trigger = alert.Trigger
		} else {
			trigger = "empty"
		}

		if alert.TriggerAt != "" {
			triggerAt = alert.TriggerAt
		} else {
			triggerAt = "empty"
		}

		if alert.TriggerType != "" {
			triggerType = alert.TriggerType
		} else {
			triggerType = "empty"
		}

		if alert.ActionSensor != "" {
			actionSensor = alert.ActionSensor
		} else {
			actionSensor = "empty"
		}

		sb.WriteString(schema.Name)

		// Tags
		sb.WriteString(`,deviceType=LNS`)
		sb.WriteString(`,deviceId=`)
		sb.WriteString(schema.DeviceId)
		// sb.WriteString(`,type=alert`)
		sb.WriteString(`,direction=`)
		sb.WriteString(direction)
		sb.WriteString(`,etc=`)
		sb.WriteString(etc)

		// unixTimestamp := alert.LastPlayed.UnixNano()
		// Fields
		sb.WriteString(` `)
		sb.WriteString(`data="`)
		sb.WriteString(alert.Data)
		sb.WriteString(`",trigger="`)
		sb.WriteString(trigger)
		sb.WriteString(`",triggerAt="`)
		sb.WriteString(triggerAt)
		sb.WriteString(`",triggerType="`)
		sb.WriteString(triggerType)
		sb.WriteString(`"`)
		// sb.WriteString(`",lastPlayed=`)
		// sb.WriteString(alert.LastPlayed)
		sb.WriteString(`,actionSensor="`)
		sb.WriteString(actionSensor)
		sb.WriteString(`",currentValue="`)
		sb.WriteString(alert.CurrentValue)
		sb.WriteString(`"`)

		// Timestamp_ms
		sb.WriteString(` `)
		sb.WriteString(strconv.FormatInt(int64(alert.Timestamp), 10))
	}

	return sb.String()
}

func parseEvseMeasurement(measurement string, data string) string {
	var sb strings.Builder

	if data == "" {
		return "No data"
	}

	switch measurement {
	case "MeterValues":
		var evseMeterValue EvseMeterValue
		json.Unmarshal([]byte(data), &evseMeterValue)

		forwardEnergy := evseMeterValue.ForwardEnergy * 0.001

		sb.WriteString(` `)
		sb.WriteString(`forwardEnergy=`)
		sb.WriteString(strconv.FormatFloat(forwardEnergy, 'f', -1, 64))

	case "StatusNotification":
		var evseStatusNotification EvseStatusNotification
		json.Unmarshal([]byte(data), &evseStatusNotification)

		sb.WriteString(`,vendorId=`)
		sb.WriteString(evseStatusNotification.VendorId)
		sb.WriteString(`,errorCode=`)
		sb.WriteString(evseStatusNotification.ErrorCode)
		// sb.WriteString(`,vendorErrorCode=`)
		// sb.WriteString(evseStatusNotification.VendorErrorCode)
		sb.WriteString(` `)
		sb.WriteString(`status="`)
		sb.WriteString(evseStatusNotification.Status)
		sb.WriteString(`"`)
		// sb.WriteString(`,info=`)
		// sb.WriteString(evseStatusNotification.Info)

	case "StartTransaction":
		var evseStartTransaction EvseStartTransaction
		json.Unmarshal([]byte(data), &evseStartTransaction)

		sb.WriteString(` `)
		sb.WriteString(`transactionId=`)
		sb.WriteString(evseStartTransaction.TransactionId)
		sb.WriteString(`,startMeter=`)
		sb.WriteString(strconv.FormatInt(evseStartTransaction.StartMeter, 10))
		sb.WriteString(`,startTime=`)
		sb.WriteString(strconv.FormatInt(evseStartTransaction.StartTime, 10))
		// sb.WriteString(`,idTag=`)
		// sb.WriteString(evseStartTransaction.IdTag)

	case "StopTransaction":
		var evseStopTransaction EvseStopTransaction
		json.Unmarshal([]byte(data), &evseStopTransaction)

		sb.WriteString(` `)
		sb.WriteString(`transactionId=`)
		sb.WriteString(evseStopTransaction.TransactionId)
		sb.WriteString(`,meterStop=`)
		sb.WriteString(strconv.FormatInt(evseStopTransaction.MeterStop, 10))
		sb.WriteString(`,stopTime=`)
		sb.WriteString(strconv.FormatInt(evseStopTransaction.StopTime, 10))
	}

	return sb.String()
}

func parseEvse(measurement string, deviceType string, deviceId string, direction string, etc string, message string) string {
	var sb strings.Builder
	var evseUp EvseUp
	var alert Alert

	if message == "" {
		return "No message to parse"
	}

	if direction == "up" {

		json.Unmarshal([]byte(message), &evseUp)

		// Measurement
		sb.WriteString(measurement)

		// Tags
		sb.WriteString(`,deviceId=`)
		sb.WriteString(evseUp.DeviceId)
		sb.WriteString(`,deviceType=`)
		sb.WriteString(deviceType)
		sb.WriteString(`,connectorId=`)
		sb.WriteString(evseUp.ConnectorId)
		sb.WriteString(`,chargePointId=`)
		sb.WriteString(evseUp.ChargePointId)
		// sb.WriteString(`,unit=`)
		// sb.WriteString(evseUp.Unit)
		// sb.WriteString(`,format=`)
		// sb.WriteString(evseUp.Format)
		// sb.WriteString(`,measurand=`)
		// sb.WriteString(evseUp.Measurand)
		// sb.WriteString(`,context=`)
		// sb.WriteString(evseUp.Context)
		// sb.WriteString(`,location=`)
		// sb.WriteString(evseUp.Location)

		sb.WriteString(`,direction=`)
		sb.WriteString(direction)
		sb.WriteString(`,origin=`)
		sb.WriteString(etc)

		// Fields
		// sb.WriteString(`,fowardEnergy=`)
		// sb.WriteString(strconv.FormatUint(evseUp.FowardEnergy, 10))
		sb.WriteString(parseEvseMeasurement(measurement, message))

		// Timestamp_ns
		sb.WriteString(` `)
		sb.WriteString(strconv.FormatInt(evseUp.Timestamp, 10))
	}

	if direction == "alert" {
		json.Unmarshal([]byte(message), &alert)

		var trigger string
		var triggerAt string
		var triggerType string
		var actionSensor string

		if alert.Trigger != "" {
			trigger = alert.Trigger
		} else {
			trigger = "empty"
		}

		if alert.TriggerAt != "" {
			triggerAt = alert.TriggerAt
		} else {
			triggerAt = "empty"
		}

		if alert.TriggerType != "" {
			triggerType = alert.TriggerType
		} else {
			triggerType = "empty"
		}

		if alert.ActionSensor != "" {
			actionSensor = alert.ActionSensor
		} else {
			actionSensor = "empty"
		}

		sb.WriteString(measurement)

		// Tags
		sb.WriteString(`,deviceType=EVSE`)
		sb.WriteString(`,deviceId=`)
		sb.WriteString(deviceId)
		// sb.WriteString(`,type=alert`)
		sb.WriteString(`,direction=`)
		sb.WriteString(direction)
		sb.WriteString(`,etc=`)
		sb.WriteString(etc)

		// unixTimestamp := alert.LastPlayed.UnixNano()
		// Fields
		sb.WriteString(` `)
		sb.WriteString(`data="`)
		sb.WriteString(alert.Data)
		sb.WriteString(`",trigger="`)
		sb.WriteString(trigger)
		sb.WriteString(`",triggerAt="`)
		sb.WriteString(triggerAt)
		sb.WriteString(`",triggerType="`)
		sb.WriteString(triggerType)
		sb.WriteString(`"`)
		// sb.WriteString(`",lastPlayed=`)
		// sb.WriteString(alert.LastPlayed)
		sb.WriteString(`,actionSensor="`)
		sb.WriteString(actionSensor)
		sb.WriteString(`",currentValue="`)
		sb.WriteString(alert.CurrentValue)
		sb.WriteString(`"`)

		// Timestamp_ms
		sb.WriteString(` `)
		sb.WriteString(strconv.FormatInt(int64(alert.Timestamp), 10))
	}
	return sb.String()
}

func parseHealthPackMeasurement(measurement string, data string) string {
	var sb strings.Builder
	var healthPackUp HealthPackUp
	var ok bool

	if data == "" {
		return "No data"
	}

	json.Unmarshal([]byte(data), &healthPackUp)

	switch measurement {
	case "Inertias":
		var healthPackInertias HealthPackInertias

		if healthPackInertias.FAccX, ok = healthPackUp.Data["fAccX"].(string); ok {
			if healthPackInertias.FAccX == "" {
				healthPackInertias.FAccX = "empty"
			}
		} else {
			return "No valid key fAccX to parse in HealthPackMeasurement - Inertias."
		}
		if healthPackInertias.FAccY, ok = healthPackUp.Data["fAccY"].(string); ok {
			if healthPackInertias.FAccY == "" {
				healthPackInertias.FAccY = "empty"
			}
		} else {
			return "No valid key fAccY to parse in HealthPackMeasurement - Inertias."
		}
		if healthPackInertias.FAccZ, ok = healthPackUp.Data["fAccZ"].(string); ok {
			if healthPackInertias.FAccZ == "" {
				healthPackInertias.FAccZ = "empty"
			}
		} else {
			return "No valid key fAccZ to parse in HealthPackMeasurement - Inertias."
		}
		if healthPackInertias.AccX, ok = healthPackUp.Data["accX"].(string); ok {
			if healthPackInertias.AccX == "" {
				healthPackInertias.AccX = "empty"
			}
		} else {
			return "No valid key accX to parse in HealthPackMeasurement - Inertias."
		}
		if healthPackInertias.AccY, ok = healthPackUp.Data["accY"].(string); ok {
			if healthPackInertias.AccY == "" {
				healthPackInertias.AccY = "empty"
			}
		} else {
			return "No valid key accY to parse in HealthPackMeasurement - Inertias."
		}
		if healthPackInertias.AccZ, ok = healthPackUp.Data["accZ"].(string); ok {
			if healthPackInertias.AccZ == "" {
				healthPackInertias.AccZ = "empty"
			}
		} else {
			return "No valid key accZ to parse in HealthPackMeasurement - Inertias."
		}
		if healthPackInertias.GyrX, ok = healthPackUp.Data["gyrX"].(string); ok {
			if healthPackInertias.GyrX == "" {
				healthPackInertias.GyrX = "empty"
			}
		} else {
			return "No valid key gyrX to parse in HealthPackMeasurement - Inertias."
		}
		if healthPackInertias.GyrY, ok = healthPackUp.Data["gyrY"].(string); ok {
			if healthPackInertias.GyrY == "" {
				healthPackInertias.GyrY = "empty"
			}
		} else {
			return "No valid key gyrY to parse in HealthPackMeasurement - Inertias."
		}
		if healthPackInertias.GyrZ, ok = healthPackUp.Data["gyrZ"].(string); ok {
			if healthPackInertias.GyrZ == "" {
				healthPackInertias.GyrZ = "empty"
			}
		} else {
			return "No valid key gyrZ to parse in HealthPackMeasurement - Inertias."
		}
		if healthPackInertias.ContimpactoX, ok = healthPackUp.Data["contimpactoX"].(string); ok {
			if healthPackInertias.ContimpactoX == "" {
				healthPackInertias.ContimpactoX = "empty"
			}
		} else {
			return "No valid key contimpactoX to parse in HealthPackMeasurement - Inertias."
		}
		if healthPackInertias.ContimpactoY, ok = healthPackUp.Data["contimpactoY"].(string); ok {
			if healthPackInertias.ContimpactoY == "" {
				healthPackInertias.ContimpactoY = "empty"
			}
		} else {
			return "No valid key contimpactoY to parse in HealthPackMeasurement - Inertias."
		}
		if healthPackInertias.ContimpactoZ, ok = healthPackUp.Data["contimpactoZ"].(string); ok {
			if healthPackInertias.ContimpactoZ == "" {
				healthPackInertias.ContimpactoZ = "empty"
			}
		} else {
			return "No valid key contimpactoZ to parse in HealthPackMeasurement - Inertias."
		}
		if healthPackInertias.Pitch, ok = healthPackUp.Data["pitch"].(string); ok {
			if healthPackInertias.Pitch == "" {
				healthPackInertias.Pitch = "empty"
			}
		} else {
			return "No valid key pitch to parse in HealthPackMeasurement - Inertias."
		}
		if healthPackInertias.Roll, ok = healthPackUp.Data["roll"].(string); ok {
			if healthPackInertias.Roll == "" {
				healthPackInertias.Roll = "empty"
			}
		} else {
			return "No valid key roll to parse in HealthPackMeasurement - Inertias."
		}
		if healthPackInertias.Yaw, ok = healthPackUp.Data["yaw"].(string); ok {
			if healthPackInertias.Yaw == "" {
				healthPackInertias.Yaw = "empty"
			}
		} else {
			return "No valid key yaw to parse in HealthPackMeasurement - Inertias."
		}

		sb.WriteString(` `)
		sb.WriteString(`fAccX=`)
		sb.WriteString(healthPackInertias.FAccX)
		sb.WriteString(`,fAccY=`)
		sb.WriteString(healthPackInertias.FAccY)
		sb.WriteString(`,fAccZ=`)
		sb.WriteString(healthPackInertias.FAccZ)
		sb.WriteString(`,accX=`)
		sb.WriteString(healthPackInertias.AccX)
		sb.WriteString(`,accY=`)
		sb.WriteString(healthPackInertias.AccY)
		sb.WriteString(`,accZ=`)
		sb.WriteString(healthPackInertias.AccZ)
		sb.WriteString(`,gyrX=`)
		sb.WriteString(healthPackInertias.GyrX)
		sb.WriteString(`,gyrY=`)
		sb.WriteString(healthPackInertias.GyrY)
		sb.WriteString(`,gyrZ=`)
		sb.WriteString(healthPackInertias.GyrZ)
		sb.WriteString(`,contimpactoX=`)
		sb.WriteString(healthPackInertias.ContimpactoX)
		sb.WriteString(`,contimpactoY=`)
		sb.WriteString(healthPackInertias.ContimpactoY)
		sb.WriteString(`,contimpactoZ=`)
		sb.WriteString(healthPackInertias.ContimpactoZ)
		sb.WriteString(`,pitch=`)
		sb.WriteString(healthPackInertias.Pitch)
		sb.WriteString(`,roll=`)
		sb.WriteString(healthPackInertias.Roll)
		sb.WriteString(`,yaw=`)
		sb.WriteString(healthPackInertias.Yaw)

	case "Tracking":
		var healthPackTracking HealthPackTracking
		json.Unmarshal([]byte(data), &healthPackTracking)

		if healthPackTracking.Latitude, ok = healthPackUp.Data["latitude"].(string); ok {
			if healthPackTracking.Latitude == "" {
				healthPackTracking.Latitude = "empty"
			}
		} else {
			return "No valid key latitude to parse in HealthPackMeasurement - Tracking."
		}
		if healthPackTracking.Longitude, ok = healthPackUp.Data["longitude"].(string); ok {
			if healthPackTracking.Longitude == "" {
				healthPackTracking.Longitude = "empty"
			}
		} else {
			return "No valid key longitude to parse in HealthPackMeasurement - Tracking."
		}
		if healthPackTracking.Tempbateriasecundaria, ok = healthPackUp.Data["tempbateriasecundaria"].(string); ok {
			if healthPackTracking.Tempbateriasecundaria == "" {
				healthPackTracking.Tempbateriasecundaria = "empty"
			}
		} else {
			return "No valid key tempbateriasecundaria to parse in HealthPackMeasurement - Tracking."
		}
		if healthPackTracking.Tempbateriaprincipal, ok = healthPackUp.Data["tempbateriaprincipal"].(string); ok {
			if healthPackTracking.Tempbateriaprincipal == "" {
				healthPackTracking.Tempbateriaprincipal = "empty"
			}
		} else {
			return "No valid key tempbateriaprincipal to parse in HealthPackMeasurement - Tracking."
		}
		if healthPackTracking.Temperaturacondensador, ok = healthPackUp.Data["temperaturacondensador"].(string); ok {
			if healthPackTracking.Temperaturacondensador == "" {
				healthPackTracking.Temperaturacondensador = "empty"
			}
		} else {
			return "No valid key temperaturacondensador to parse in HealthPackMeasurement - Tracking."
		}
		if healthPackTracking.Temperaturacuba1, ok = healthPackUp.Data["temperaturacuba1"].(string); ok {
			if healthPackTracking.Temperaturacuba1 == "" {
				healthPackTracking.Temperaturacuba1 = "empty"
			}
		} else {
			return "No valid key temperaturacuba1 to parse in HealthPackMeasurement - Tracking."
		}
		if healthPackTracking.Temperaturacuba2, ok = healthPackUp.Data["temperaturacuba2"].(string); ok {
			if healthPackTracking.Temperaturacuba2 == "" {
				healthPackTracking.Temperaturacuba2 = "empty"
			}
		} else {
			return "No valid key temperaturacuba2 to parse in HealthPackMeasurement - Tracking."
		}
		if healthPackTracking.TemperaturaexternaLL, ok = healthPackUp.Data["temperaturaexternaLL"].(string); ok {
			if healthPackTracking.TemperaturaexternaLL == "" {
				healthPackTracking.TemperaturaexternaLL = "empty"
			}
		} else {
			return "No valid key temperaturaexternaLL to parse in HealthPackMeasurement - Tracking."
		}
		if healthPackTracking.TemperaturaexternaLS, ok = healthPackUp.Data["temperaturaexternaLS"].(string); ok {
			if healthPackTracking.TemperaturaexternaLS == "" {
				healthPackTracking.TemperaturaexternaLS = "empty"
			}
		} else {
			return "No valid key temperaturaexternaLS to parse in HealthPackMeasurement - Tracking."
		}
		if healthPackTracking.Temperaturaexterna, ok = healthPackUp.Data["temperaturaexterna"].(string); ok {
			if healthPackTracking.Temperaturaexterna == "" {
				healthPackTracking.Temperaturaexterna = "empty"
			}
		} else {
			return "No valid key temperaturaexterna to parse in HealthPackMeasurement - Tracking."
		}
		if healthPackTracking.Temperaturadissipador, ok = healthPackUp.Data["temperaturadissipador"].(string); ok {
			if healthPackTracking.Temperaturadissipador == "" {
				healthPackTracking.Temperaturadissipador = "empty"
			}
		} else {
			return "No valid key temperaturadissipador to parse in HealthPackMeasurement - Tracking."
		}
		if healthPackTracking.Correntebateria, ok = healthPackUp.Data["correntebateria"].(string); ok {
			if healthPackTracking.Correntebateria == "" {
				healthPackTracking.Correntebateria = "empty"
			}
		} else {
			return "No valid key correntebateria to parse in HealthPackMeasurement - Tracking."
		}
		if healthPackTracking.Correntecompressor, ok = healthPackUp.Data["correntecompressor"].(string); ok {
			if healthPackTracking.Correntecompressor == "" {
				healthPackTracking.Correntecompressor = "empty"
			}
		} else {
			return "No valid key correntecompressor to parse in HealthPackMeasurement - Tracking."
		}
		if healthPackTracking.Correntepeltier, ok = healthPackUp.Data["correntepeltier"].(string); ok {
			if healthPackTracking.Correntepeltier == "" {
				healthPackTracking.Correntepeltier = "empty"
			}
		} else {
			return "No valid key correntepeltier to parse in HealthPackMeasurement - Tracking."
		}
		if healthPackTracking.Correntecooler, ok = healthPackUp.Data["correntecooler"].(string); ok {
			if healthPackTracking.Correntecooler == "" {
				healthPackTracking.Correntecooler = "empty"
			}
		} else {
			return "No valid key correntecooler to parse in HealthPackMeasurement - Tracking."
		}
		if healthPackTracking.Correnteexaustor, ok = healthPackUp.Data["correnteexaustor"].(string); ok {
			if healthPackTracking.Correnteexaustor == "" {
				healthPackTracking.Correnteexaustor = "empty"
			}
		} else {
			return "No valid key correnteexaustor to parse in HealthPackMeasurement - Tracking."
		}
		if healthPackTracking.Temperaturacompressor, ok = healthPackUp.Data["temperaturacompressor"].(string); ok {
			if healthPackTracking.Temperaturacompressor == "" {
				healthPackTracking.Temperaturacompressor = "empty"
			}
		} else {
			return "No valid key temperaturacompressor to parse in HealthPackMeasurement - Tracking."
		}
		if healthPackTracking.Setpoint_pid1, ok = healthPackUp.Data["setpoint_pid1"].(string); ok {
			if healthPackTracking.Setpoint_pid1 == "" {
				healthPackTracking.Setpoint_pid1 = "empty"
			}
		} else {
			return "No valid key setpoint_pid1 to parse in HealthPackMeasurement - Tracking."
		}
		if healthPackTracking.Valor_pid1_atual, ok = healthPackUp.Data["valor_pid1_atual"].(string); ok {
			if healthPackTracking.Valor_pid1_atual == "" {
				healthPackTracking.Valor_pid1_atual = "empty"
			}
		} else {
			return "No valid key valor_pid1_atual to parse in HealthPackMeasurement - Tracking."
		}
		if healthPackTracking.Esforco_pid1, ok = healthPackUp.Data["esforco_pid1"].(string); ok {
			if healthPackTracking.Esforco_pid1 == "" {
				healthPackTracking.Esforco_pid1 = "empty"
			}
		} else {
			return "No valid key esforco_pid1 to parse in HealthPackMeasurement - Tracking."
		}
		if healthPackTracking.Setpoint_pid2, ok = healthPackUp.Data["setpoint_pid2"].(string); ok {
			if healthPackTracking.Setpoint_pid2 == "" {
				healthPackTracking.Setpoint_pid2 = "empty"
			}
		} else {
			return "No valid key setpoint_pid2 to parse in HealthPackMeasurement - Tracking."
		}
		if healthPackTracking.Valor_pid2_atual, ok = healthPackUp.Data["valor_pid2_atual"].(string); ok {
			if healthPackTracking.Valor_pid2_atual == "" {
				healthPackTracking.Valor_pid2_atual = "empty"
			}
		} else {
			return "No valid key valor_pid2_atual to parse in HealthPackMeasurement - Tracking."
		}
		if healthPackTracking.Esforco_pid2, ok = healthPackUp.Data["esforco_pid2"].(string); ok {
			if healthPackTracking.Esforco_pid2 == "" {
				healthPackTracking.Esforco_pid2 = "empty"
			}
		} else {
			return "No valid key esforco_pid2 to parse in HealthPackMeasurement - Tracking."
		}

		sb.WriteString(` `)
		sb.WriteString(`latitude=`)
		sb.WriteString(healthPackTracking.Latitude)
		sb.WriteString(`,longitude=`)
		sb.WriteString(healthPackTracking.Longitude)
		sb.WriteString(`,tempbateriasecundaria=`)
		sb.WriteString(healthPackTracking.Tempbateriasecundaria)
		sb.WriteString(`,tempbateriaprincipal=`)
		sb.WriteString(healthPackTracking.Tempbateriaprincipal)
		sb.WriteString(`,temperaturacondensador=`)
		sb.WriteString(healthPackTracking.Temperaturacondensador)
		sb.WriteString(`,temperaturacuba1=`)
		sb.WriteString(healthPackTracking.Temperaturacuba1)
		sb.WriteString(`,temperaturacuba2=`)
		sb.WriteString(healthPackTracking.Temperaturacuba2)
		sb.WriteString(`,temperaturaexternaLL=`)
		sb.WriteString(healthPackTracking.TemperaturaexternaLL)
		sb.WriteString(`,temperaturaexternaLS=`)
		sb.WriteString(healthPackTracking.TemperaturaexternaLS)
		sb.WriteString(`,temperaturaexterna=`)
		sb.WriteString(healthPackTracking.Temperaturaexterna)
		sb.WriteString(`,temperaturadissipador=`)
		sb.WriteString(healthPackTracking.Temperaturadissipador)
		sb.WriteString(`,correntebateria=`)
		sb.WriteString(healthPackTracking.Correntebateria)
		sb.WriteString(`,correntecompressor=`)
		sb.WriteString(healthPackTracking.Correntecompressor)
		sb.WriteString(`,correntepeltier=`)
		sb.WriteString(healthPackTracking.Correntepeltier)
		sb.WriteString(`,correntecooler=`)
		sb.WriteString(healthPackTracking.Correntecooler)
		sb.WriteString(`,correnteexaustor=`)
		sb.WriteString(healthPackTracking.Correnteexaustor)
		sb.WriteString(`,temperaturacompressor=`)
		sb.WriteString(healthPackTracking.Temperaturacompressor)
		sb.WriteString(`,setpoint_pid1=`)
		sb.WriteString(healthPackTracking.Setpoint_pid1)
		sb.WriteString(`,valor_pid1_atual=`)
		sb.WriteString(healthPackTracking.Valor_pid1_atual)
		sb.WriteString(`,esforco_pid1=`)
		sb.WriteString(healthPackTracking.Esforco_pid1)
		sb.WriteString(`,setpoint_pid2=`)
		sb.WriteString(healthPackTracking.Setpoint_pid2)
		sb.WriteString(`,valor_pid2_atual=`)
		sb.WriteString(healthPackTracking.Valor_pid2_atual)
		sb.WriteString(`,esforco_pid2=`)
		sb.WriteString(healthPackTracking.Esforco_pid2)

	case "Status":
		var healthPackStatus HealthPackStatus
		json.Unmarshal([]byte(data), &healthPackStatus)

		if healthPackStatus.Vbateriaprincipal, ok = healthPackUp.Data["vbateriaprincipal"].(string); ok {
			if healthPackStatus.Vbateriaprincipal == "" {
				healthPackStatus.Vbateriaprincipal = "empty"
			}
		} else {
			return "No valid key vbateriaprincipal to parse in HealthPackMeasurement - Status."
		}
		if healthPackStatus.Vbateriasecundaria, ok = healthPackUp.Data["vbateriasecundaria"].(string); ok {
			if healthPackStatus.Vbateriasecundaria == "" {
				healthPackStatus.Vbateriasecundaria = "empty"
			}
		} else {
			return "No valid key vbateriasecundaria to parse in HealthPackMeasurement - Status."
		}
		if healthPackStatus.Ventradafonteexterna, ok = healthPackUp.Data["ventradafonteexterna"].(string); ok {
			if healthPackStatus.Ventradafonteexterna == "" {
				healthPackStatus.Ventradafonteexterna = "empty"
			}
		} else {
			return "No valid key ventradafonteexterna to parse in HealthPackMeasurement - Status."
		}
		if healthPackStatus.Numerocaixa, ok = healthPackUp.Data["numerocaixa"].(string); ok {
			if healthPackStatus.Numerocaixa == "" {
				healthPackStatus.Numerocaixa = "empty"
			}
		} else {
			return "No valid key numerocaixa to parse in HealthPackMeasurement - Status."
		}
		if healthPackStatus.Estadomaquina, ok = healthPackUp.Data["estadomaquina"].(string); ok {
			if healthPackStatus.Estadomaquina == "" {
				healthPackStatus.Estadomaquina = "empty"
			}
		} else {
			return "No valid key estadomaquina to parse in HealthPackMeasurement - Status."
		}
		if healthPackStatus.Timestamp, ok = healthPackUp.Data["timestamp"].(string); ok {
			if healthPackStatus.Timestamp == "" {
				healthPackStatus.Timestamp = "empty"
			}
		} else {
			return "No valid key timestamp to parse in HealthPackMeasurement - Status."
		}
		if healthPackStatus.IdFalha, ok = healthPackUp.Data["idFalha"].(string); ok {
			if healthPackStatus.IdFalha == "" {
				healthPackStatus.IdFalha = "empty"
			}
		} else {
			return "No valid key idFalha to parse in HealthPackMeasurement - Status."
		}
		if healthPackStatus.Porcentagemsinalcomunicacao, ok = healthPackUp.Data["porcentagemsinalcomunicacao"].(string); ok {
			if healthPackStatus.Porcentagemsinalcomunicacao == "" {
				healthPackStatus.Porcentagemsinalcomunicacao = "empty"
			}
		} else {
			return "No valid key porcentagemsinalcomunicacao to parse in HealthPackMeasurement - Status."
		}
		if healthPackStatus.FatorRH, ok = healthPackUp.Data["fatorRH"].(string); ok {
			if healthPackStatus.FatorRH == "" {
				healthPackStatus.FatorRH = "empty"
			}
		} else {
			return "No valid key fatorRH to parse in HealthPackMeasurement - Status."
		}
		if healthPackStatus.Btdown, ok = healthPackUp.Data["btdown"].(string); ok {
			if healthPackStatus.Btdown == "" {
				healthPackStatus.Btdown = "empty"
			}
		} else {
			return "No valid key btdown to parse in HealthPackMeasurement - Status."
		}
		if healthPackStatus.Btselect, ok = healthPackUp.Data["btselect"].(string); ok {
			if healthPackStatus.Btselect == "" {
				healthPackStatus.Btselect = "empty"
			}
		} else {
			return "No valid key btselect to parse in HealthPackMeasurement - Status."
		}
		if healthPackStatus.Btup, ok = healthPackUp.Data["btup"].(string); ok {
			if healthPackStatus.Btup == "" {
				healthPackStatus.Btup = "empty"
			}
		} else {
			return "No valid key btup to parse in HealthPackMeasurement - Status."
		}
		if healthPackStatus.Tecla_enter, ok = healthPackUp.Data["tecla_enter"].(string); ok {
			if healthPackStatus.Tecla_enter == "" {
				healthPackStatus.Tecla_enter = "empty"
			}
		} else {
			return "No valid key tecla_enter to parse in HealthPackMeasurement - Status."
		}
		if healthPackStatus.Statustampaprincipal, ok = healthPackUp.Data["statustampaprincipal"].(string); ok {
			if healthPackStatus.Statustampaprincipal == "" {
				healthPackStatus.Statustampaprincipal = "empty"
			}
		} else {
			return "No valid key statustampaprincipal to parse in HealthPackMeasurement - Status."
		}
		if healthPackStatus.Statustampaprincipal, ok = healthPackUp.Data["statustampaprincipal"].(string); ok {
			if healthPackStatus.Statustampaprincipal == "" {
				healthPackStatus.Statustampaprincipal = "empty"
			}
		} else {
			return "No valid key statustampaprincipal to parse in HealthPackMeasurement - Status."
		}
		if healthPackStatus.Statusserialprincipal, ok = healthPackUp.Data["statusserialprincipal"].(string); ok {
			if healthPackStatus.Statusserialprincipal == "" {
				healthPackStatus.Statusserialprincipal = "empty"
			}
		} else {
			return "No valid key statusserialprincipal to parse in HealthPackMeasurement - Status."
		}
		if healthPackStatus.Statusserialsecundaria, ok = healthPackUp.Data["statusserialsecundaria"].(string); ok {
			if healthPackStatus.Statusserialsecundaria == "" {
				healthPackStatus.Statusserialsecundaria = "empty"
			}
		} else {
			return "No valid key statusserialsecundaria to parse in HealthPackMeasurement - Status."
		}
		if healthPackStatus.Statustampacasamaq, ok = healthPackUp.Data["statustampacasamaq"].(string); ok {
			if healthPackStatus.Statustampacasamaq == "" {
				healthPackStatus.Statustampacasamaq = "empty"
			}
		} else {
			return "No valid key statustampacasamaq to parse in HealthPackMeasurement - Status."
		}
		if healthPackStatus.Controle_peltier, ok = healthPackUp.Data["controle_peltier"].(string); ok {
			if healthPackStatus.Controle_peltier == "" {
				healthPackStatus.Controle_peltier = "empty"
			}
		} else {
			return "No valid key controle_peltier to parse in HealthPackMeasurement - Status."
		}
		if healthPackStatus.Porcentagem_bat, ok = healthPackUp.Data["porcentagem_bat"].(string); ok {
			if healthPackStatus.Porcentagem_bat == "" {
				healthPackStatus.Porcentagem_bat = "empty"
			}
		} else {
			return "No valid key porcentagem_bat to parse in HealthPackMeasurement - Status."
		}

		sb.WriteString(` `)
		sb.WriteString(`vbateriaprincipal=`)
		sb.WriteString(healthPackStatus.Vbateriaprincipal)
		sb.WriteString(`,vbateriasecundaria=`)
		sb.WriteString(healthPackStatus.Vbateriasecundaria)
		sb.WriteString(`,ventradafonteexterna=`)
		sb.WriteString(healthPackStatus.Ventradafonteexterna)
		sb.WriteString(`,numerocaixa=`)
		sb.WriteString(healthPackStatus.Numerocaixa)
		sb.WriteString(`,estadomaquina=`)
		sb.WriteString(healthPackStatus.Estadomaquina)
		sb.WriteString(`,timestamp=`)
		sb.WriteString(healthPackStatus.Timestamp)
		sb.WriteString(`,idFalha=`)
		sb.WriteString(healthPackStatus.IdFalha)
		sb.WriteString(`,porcentagemsinalcomunicacao=`)
		sb.WriteString(healthPackStatus.Porcentagemsinalcomunicacao)
		sb.WriteString(`,fatorRH=`)
		sb.WriteString(healthPackStatus.FatorRH)
		sb.WriteString(`,btdown=`)
		sb.WriteString(healthPackStatus.Btdown)
		sb.WriteString(`,btselect=`)
		sb.WriteString(healthPackStatus.Btselect)
		sb.WriteString(`,btup=`)
		sb.WriteString(healthPackStatus.Btup)
		sb.WriteString(`,tecla_enter=`)
		sb.WriteString(healthPackStatus.Tecla_enter)
		sb.WriteString(`,statustampaprincipal=`)
		sb.WriteString(healthPackStatus.Statustampaprincipal)
		sb.WriteString(`,statusserialprincipal=`)
		sb.WriteString(healthPackStatus.Statusserialprincipal)
		sb.WriteString(`,statusserialsecundaria=`)
		sb.WriteString(healthPackStatus.Statusserialsecundaria)
		sb.WriteString(`,statustampacasamaq=`)
		sb.WriteString(healthPackStatus.Statustampacasamaq)
		sb.WriteString(`,controle_peltier=`)
		sb.WriteString(healthPackStatus.Controle_peltier)
		sb.WriteString(`,porcentagem_bat=`)
		sb.WriteString(healthPackStatus.Porcentagem_bat)

	case "Ischemia":
		var healthPackIschemia HealthPackIschemia
		json.Unmarshal([]byte(data), &healthPackIschemia)

		if healthPackIschemia.IdModal, ok = healthPackUp.Data["idModal"].(string); ok {
			if healthPackIschemia.IdModal == "" {
				healthPackIschemia.IdModal = "empty"
			}
		} else {
			return "No valid key to parse in HealthPackMeasurement - Ischemia."
		}
		if healthPackIschemia.IdOperador, ok = healthPackUp.Data["IdOperador"].(string); ok {
			if healthPackIschemia.IdOperador == "" {
				healthPackIschemia.IdOperador = "empty"
			}
		} else {
			return "No valid key to parse in HealthPackMeasurement - Ischemia."
		}
		if healthPackIschemia.Niveldepermissao, ok = healthPackUp.Data["niveldepermissao"].(string); ok {
			if healthPackIschemia.Niveldepermissao == "" {
				healthPackIschemia.Niveldepermissao = "empty"
			}
		} else {
			return "No valid key to parse in HealthPackMeasurement - Ischemia."
		}
		if healthPackIschemia.Nome, ok = healthPackUp.Data["nome"].(string); ok {
			if healthPackIschemia.Nome == "" {
				healthPackIschemia.Nome = "empty"
			}
		} else {
			return "No valid key to parse in HealthPackMeasurement - Ischemia."
		}
		if healthPackIschemia.Numtransplante, ok = healthPackUp.Data["numtransplante"].(string); ok {
			if healthPackIschemia.Numtransplante == "" {
				healthPackIschemia.Numtransplante = "empty"
			}
		} else {
			return "No valid key to parse in HealthPackMeasurement - Ischemia."
		}
		if healthPackIschemia.Numeroempresa, ok = healthPackUp.Data["numeroempresa"].(string); ok {
			if healthPackIschemia.Numeroempresa == "" {
				healthPackIschemia.Numeroempresa = "empty"
			}
		} else {
			return "No valid key to parse in HealthPackMeasurement - Ischemia."
		}
		if healthPackIschemia.Orgao, ok = healthPackUp.Data["orgao"].(string); ok {
			if healthPackIschemia.Orgao == "" {
				healthPackIschemia.Orgao = "empty"
			}
		} else {
			return "No valid key to parse in HealthPackMeasurement - Ischemia."
		}
		if healthPackIschemia.Tempo_total_isquemia, ok = healthPackUp.Data["tempo_total_isquemia"].(string); ok {
			if healthPackIschemia.Tempo_total_isquemia == "" {
				healthPackIschemia.Tempo_total_isquemia = "Tempo_total_isquemia"
			}
		} else {
			return "No valid key to parse in HealthPackMeasurement - Ischemia."
		}
		if healthPackIschemia.Tempo_restante_isquemia, ok = healthPackUp.Data["tempo_restante_isquemia"].(string); ok {
			if healthPackIschemia.Tempo_restante_isquemia == "" {
				healthPackIschemia.Tempo_restante_isquemia = "Tempo_restante_isquemia"
			}
		} else {
			return "No valid key to parse in HealthPackMeasurement - Ischemia."
		}
		if healthPackIschemia.Hora_isquemia, ok = healthPackUp.Data["hora_isquemia"].(string); ok {
			if healthPackIschemia.Hora_isquemia == "" {
				healthPackIschemia.Hora_isquemia = "empty"
			}
		} else {
			return "No valid key to parse in HealthPackMeasurement - Ischemia."
		}
		if healthPackIschemia.Timeinfo_sp2, ok = healthPackUp.Data["timeinfo_sp2"].(string); ok {
			if healthPackIschemia.Timeinfo_sp2 == "" {
				healthPackIschemia.Timeinfo_sp2 = "empty"
			}
		} else {
			return "No valid key to parse in HealthPackMeasurement - Ischemia."
		}

		// sb.WriteString(` `)
		// sb.WriteString(`idModal=`)
		// sb.WriteString(healthPackIschemia.IdModal)
		// sb.WriteString(`,IdOperador=`)
		// sb.WriteString(healthPackIschemia.IdOperador)
		// sb.WriteString(`,niveldepermissao=`)
		// sb.WriteString(healthPackIschemia.Niveldepermissao)
		// sb.WriteString(`,nome=`)
		// sb.WriteString(healthPackIschemia.Nome)
		// sb.WriteString(`,numtransplante=`)
		// sb.WriteString(healthPackIschemia.Numtransplante)
		// sb.WriteString(`,numeroempresa=`)
		// sb.WriteString(healthPackIschemia.Numeroempresa)
		// sb.WriteString(`,orgao=`)
		// sb.WriteString(healthPackIschemia.Orgao)
		// sb.WriteString(`,tempo_total_isquemia=`)
		// sb.WriteString(healthPackIschemia.Tempo_total_isquemia)
		// sb.WriteString(`,tempo_restante_isquemia=`)
		// sb.WriteString(healthPackIschemia.Tempo_restante_isquemia)
		// sb.WriteString(`,hora_isquemia=`)
		// sb.WriteString(healthPackIschemia.Hora_isquemia)
		// sb.WriteString(`,timeinfo_sp2=`)
		// sb.WriteString(healthPackIschemia.Timeinfo_sp2)

		// case "Alarm":
		// 	var healthPackAlarm HealthPackAlarm
		// 	json.Unmarshal([]byte(data), &healthPackAlarm)

		// // TODO: Verify if alarm is not nil, then poarse it!
		// sb.WriteString(`,alarms=`)
		// sb.WriteString(healthPackAlarm.Alarms)

	}

	return sb.String()
}

func parseHealthPack(measurement string, deviceType string, deviceId string, direction string, etc string, message string) string {
	var sb strings.Builder
	var healthPackUp HealthPackUp
	var healthPackUpProps HealthPackUpProps

	// TODO: SET ALL TIMES TO TIMESTAMP IN NS

	if message == "" {
		return "No message to parse"
	}

	if direction == "up" {
		var setUTC strings.Builder

		// JSON to healthPackUp struct
		json.Unmarshal([]byte(message), &healthPackUp)
		healthPackUpProps.DeviceName = healthPackUp.Props.DeviceName
		healthPackUpProps.DeviceIp = healthPackUp.Props.DeviceIp
		healthPackUpProps.MacAddress = healthPackUp.Props.MacAddress

		sb.WriteString(measurement)

		// Tags
		sb.WriteString(`,deviceId=`)
		sb.WriteString(healthPackUpProps.DeviceName)
		sb.WriteString(`,deviceType=`)
		sb.WriteString(deviceType)
		sb.WriteString(`,macAddress=`)
		sb.WriteString(healthPackUpProps.MacAddress)
		sb.WriteString(`,deviceIp=`)
		sb.WriteString(healthPackUpProps.DeviceIp)

		sb.WriteString(`,direction=`)
		sb.WriteString(direction)
		sb.WriteString(`,origin=`)
		sb.WriteString(etc)

		// Fields
		sb.WriteString(parseHealthPackMeasurement(measurement, message))

		// Timestamp_ns
		sb.WriteString(` `)

		dateString := healthPackUp.Date
		setUTC.WriteString("20")
		setUTC.WriteString(dateString)
		setUTC.WriteString(" -0300")

		layout := "2006:01:02 15:04:05 -0700"

		t, err := time.Parse(layout, setUTC.String())
		if err != nil {
			fmt.Println("Error parsing date:", err)
		}

		sb.WriteString(strconv.FormatInt(t.UnixNano(), 10))
	}

	return sb.String()
}

func parseNspiMeasurement(measurement string, data string) string {
	var sb strings.Builder

	if data == "" {
		return "No data"
	}

	switch measurement {
	case "GenericJson":
		var nspiGenericJson NspiGenericJson
		json.Unmarshal([]byte(data), &nspiGenericJson)

		// TODO -> Assign strings to Tags and not strings into fields
		sb.WriteString(` `)
		sb.WriteString(`data=`)
		sb.WriteString(nspiGenericJson.Data)
	}

	return sb.String()
}

func parseNspi(measurement string, deviceType string, deviceId string, direction string, etc string, message string) string {
	var sb strings.Builder
	var nspiUp NspiUp

	if message == "" {
		return "No message to parse"
	}

	if direction == "up" {
		var measurement strings.Builder

		json.Unmarshal([]byte(message), &nspiUp)

		// Measurement
		sb.WriteString(measurement.String())

		// Tags
		sb.WriteString(`,deviceId=`)
		sb.WriteString(nspiUp.DeviceId)
		sb.WriteString(`,deviceType=`)
		sb.WriteString(deviceType)
		// sb.WriteString(`,connectorId="`)
		// sb.WriteString(evseUp.ConnectorId)
		// sb.WriteString(`",chargePointId=`)
		// sb.WriteString(evseUp.ChargePointId)
		// sb.WriteString(`,unit=`)
		// sb.WriteString(evseUp.Unit)
		// sb.WriteString(`,format=`)
		// sb.WriteString(evseUp.Format)
		// sb.WriteString(`,measurand=`)
		// sb.WriteString(evseUp.Measurand)
		// sb.WriteString(`,context=`)
		// sb.WriteString(evseUp.Context)
		// sb.WriteString(`,location=`)
		// sb.WriteString(evseUp.Location)

		sb.WriteString(`,direction=`)
		sb.WriteString(direction)
		sb.WriteString(`,origin=`)
		sb.WriteString(etc)

		// Fields
		// sb.WriteString(`,fowardEnergy=`)
		// sb.WriteString(strconv.FormatUint(evseUp.FowardEnergy, 10))
		sb.WriteString(parseNspiMeasurement(measurement.String(), message))

		// Timestamp_ns
		sb.WriteString(` `)
		sb.WriteString(strconv.FormatInt(nspiUp.Timestamp, 10))
	}
	return sb.String()
}

func connLostHandler(c MQTT.Client, err error) {
	fmt.Printf("Connection lost, reason: %v\n", err)
	os.Exit(1)
}

func main() {

	file, err := os.Open("schema.json")
	if err != nil {
		fmt.Println(err)
		return
	}
	defer file.Close()

	decoder := json.NewDecoder(file)

	var data map[string]interface{}
	err = decoder.Decode(&data)
	if err != nil {
		fmt.Println(err)
		return
	}

	// Print the decoded JSON data
	fmt.Println(data)

	id := uuid.New().String()
	// ORGANIZATION := os.Getenv("ORGANIZATION")
	// DEVICE_TYPE := os.Getenv("DEVICE_TYPE")
	BUCKET := os.Getenv("BUCKET")
	MQTT_BROKER := os.Getenv("MQTT_BROKER")
	kafkaBroker := os.Getenv("KAFKA_BROKER")

	// MqttSubscriberClient
	var sbMqttSubClientId strings.Builder
	sbMqttSubClientId.WriteString("parse-lns-sub-")
	sbMqttSubClientId.WriteString(id)

	// MqttSubscriberTopic
	var sbMqttSubTopic strings.Builder
	// sbMqttSubTopic.WriteString("debug/OpenDataTelemetry/")
	sbMqttSubTopic.WriteString("OpenDataTelemetry/")
	// sbMqttSubTopic.WriteString(ORGANIZATION)
	// sbMqttSubTopic.WriteString("/")
	// sbMqttSubTopic.WriteString(DEVICE_TYPE)
	sbMqttSubTopic.WriteString("+/+/+/+/+/+")
	// sbMqttSubTopic.WriteString("#")
	// sbMqttSubTopic.WriteString("/+/+/+")

	// KafkaProducerClient
	var sbKafkaProdClientId strings.Builder
	sbKafkaProdClientId.WriteString("parse-lns-prod-")
	sbKafkaProdClientId.WriteString(id)

	// MQTT
	mqttSubBroker := MQTT_BROKER
	mqttSubClientId := sbMqttSubClientId.String()
	mqttSubUser := "public"
	mqttSubPassword := "public"
	mqttSubQos := 0

	mqttSubOpts := MQTT.NewClientOptions()
	mqttSubOpts.AddBroker(mqttSubBroker)
	mqttSubOpts.SetClientID(mqttSubClientId)
	mqttSubOpts.SetUsername(mqttSubUser)
	mqttSubOpts.SetPassword(mqttSubPassword)
	mqttSubOpts.SetConnectionLostHandler(connLostHandler)

	c := make(chan [2]string)

	mqttSubOpts.SetDefaultPublishHandler(func(mqttClient MQTT.Client, msg MQTT.Message) {
		c <- [2]string{msg.Topic(), string(msg.Payload())}
	})

	mqttSubClient := MQTT.NewClient(mqttSubOpts)
	if token := mqttSubClient.Connect(); token.Wait() && token.Error() != nil {
		panic(token.Error())
	} else {
		fmt.Printf("Connected to %s\n", mqttSubBroker)
	}

	if token := mqttSubClient.Subscribe(sbMqttSubTopic.String(), byte(mqttSubQos), nil); token.Wait() && token.Error() != nil {
		fmt.Println(token.Error())
		os.Exit(1)
	}

	// KAFKA
	// kafkaProdClient, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": "my-cluster-kafka-bootstrap.test-kafka.svc.cluster.local"})
	kafkaProdClient, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": kafkaBroker})
	if err != nil {
		panic(err)
	}
	defer kafkaProdClient.Close()

	// Delivery report handler for produced messages
	go func() {
		for e := range kafkaProdClient.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					fmt.Printf("Delivery failed: %v\n", ev.TopicPartition)
				} else {
					fmt.Printf("\nDelivered message to %v\n", ev.TopicPartition)
				}
			}
		}
	}()

	// MQTT -> KAFKA
	for {
		// 1. Input
		incoming := <-c

		// 2. Process
		// 2.1. Process Topic
		s := strings.Split(incoming[0], "/")

		// JSON SCHEMA
		// jsonSchema := `{
		// 	"deviceId": "000459282",
		// 	"name": "SmartLight",
		// 	"tags": {
		// 		"deviceId": "000459282",
		// 		"organization": "IMT",
		// 		"deviceType": "LNS",
		// 		"deviceName": "SmartLight_1",
		// 		"deviceLocation": "R210"
		// 	},
		// 	"fields": {
		// 		"temperature": {
		// 			"scale": 0.1,
		// 			"offset": 0
		// 		},
		// 		"humidity": {
		// 			"scale": 0.1,
		// 			"offset": 0
		// 		},
		// 		"boardVoltage": {
		// 			"scale": 0.1,
		// 			"offset": 0
		// 		},
		// 		"movement": {
		// 			"scale": 0.1,
		// 			"offset": 0
		// 		},
		// 		"luminosity": {
		// 			"scale": 0.1,
		// 			"offset": 0
		// 		},
		// 		"batteryVoltage": {
		// 			"scale": 0.1,
		// 			"offset": 0
		// 		}
		// 	}
		// }`

		var schema Schema
		json.Unmarshal(data["schema"].([]byte), &schema)

		// OpenDataTelemetry/IMT/LNS/MEASUREMENT/DEVICE_ID/up/imt
		// OpenDataTelemetry/IMT/LNS/MEASUREMENT/DEVICE_ID/down/chirpstackv4
		organization := s[1]
		deviceType := s[2]  // get from schema
		measurement := s[3] // get from schema
		deviceId := s[4]    // query the schema SELECT FROM schema WHERE tags.deviceId = s[4]
		direction := s[5]
		etc := s[6]

		// json := imt-schema.json
		// deviceType = json.deviceType
		// measurement = json.measurement

		// // DEBUG
		// measurement := s[4]
		// deviceId := s[5]
		// direction := s[6]
		// etc := s[7]

		var kafkaMessage string

		// Data TAG_KEYS shall be given by the application API using a Redis database. So the correct information shall be stored alongside with sensor
		// MAP deviceId vs deviceType to understand what decode really means for each one then write to kafka after decoded
		// Parse (IMT vs ATC) -> Map (deviceId vs deviceType) -> decode payload by port (0dCCCCCC)
		// -> measurement=,tag_key1=,tag_key2=, field_key_1= timestamp_ms
		// smartlight, raw=,temperature=,humidity=,lux=,movement=,box_battery=,board_voltage= timestamp_ms
		// gaugepressure, raw=pressure_in=,pressure_out=,board_voltage= timestamp_ms
		// watertanklevel, raw=distance=,pressure_out=,board_voltage= timestamp_ms
		// healthpack_alarm, raw=emergency= timestamp_ms
		// healthpack_tracking, raw=latitude=,longitude= timestamp_ms
		// evse_startTransaction, raw= timestamp_ms
		// evse_heartbeat, raw= timestamp_ms

		switch organization {
		case "IMT":
			switch deviceType {
			case "LNS":
				// kafkaMessage = parseLns(measurement, deviceId, direction, etc, incoming[1])
				kafkaMessage = parseLns(schema, direction, etc, incoming[1])

			case "EVSE":
				kafkaMessage = parseEvse(measurement, deviceType, deviceId, direction, etc, incoming[1])

			case "NSPI":
				kafkaMessage = parseNspi(measurement, deviceType, deviceId, direction, etc, incoming[1])

			case "HealthPack":
				kafkaMessage = parseHealthPack(measurement, deviceType, deviceId, direction, etc, incoming[1])

			default:
			}
		case "SaoRafael":
			switch deviceType {
			case "HealthPack":
				kafkaMessage = parseHealthPack(measurement, deviceType, deviceId, direction, etc, incoming[1])
			}
		}

		fmt.Printf("\n>>>>")
		fmt.Printf("\nTopic: %s", incoming[0])
		fmt.Printf("\nMessage: %s", kafkaMessage)
		fmt.Printf("\n>>>>")

		// SET KAFKA
		// KafkaProducerClient
		var sbKafkaProdTopic strings.Builder
		// TODO : parse by ORGANIZATION
		// sbKafkaProdTopic.WriteString(organization)
		sbKafkaProdTopic.WriteString("IMT")
		sbKafkaProdTopic.WriteString(".")
		sbKafkaProdTopic.WriteString(BUCKET)
		kafkaProdTopic := sbKafkaProdTopic.String()
		// pClient.Publish(sbPubTopic.String(), byte(pQos), false, incoming[1])

		kafkaProdClient.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &kafkaProdTopic, Partition: kafka.PartitionAny},
			Value:          []byte(kafkaMessage),
			Headers:        []kafka.Header{{Key: "myTestHeader", Value: []byte("header values are binary")}},
		}, nil)
		if err != nil {
			fmt.Printf("Produce failed: %v\n", err)
			os.Exit(1)
		}

		kafkaProdClient.Flush(15 * 1000)
	}
}
