use solana_rbpf::ebpf;
use solana_rbpf::ebpf::get_insn;
use solana_rbpf::elf::Executable;
use solana_rbpf::error::UserDefinedError;
use solana_rbpf::vm::{Config, InstructionMeter, SyscallRegistry};
use std::error::Error;
use std::fmt::{Debug, Display, Formatter};
use std::io::{stdin, stdout, Read, Stdin, Write};
use std::ops::AddAssign;
use tar::{Archive, Entry};

fn main() {
    env_logger::init_from_env(
        env_logger::Env::default().filter_or(env_logger::DEFAULT_FILTER_ENV, "info"),
    );
    if let Err(e) = _main() {
        eprintln!("{}", e);
        std::process::exit(1);
    }
}

pub type GenericResult<T> = Result<T, Box<dyn Error>>;

fn _main() -> GenericResult<()> {
    let stats = create_stats()?;
    stats.write_csv(stdout())?;
    Ok(())
}

fn create_stats() -> GenericResult<OpcodeStats> {
    let mut archive = Archive::new(stdin());
    let mut elf_buffer = Vec::<u8>::new();
    let mut stats = OpcodeStats::default();
    for entry in archive.entries()? {
        let entry = entry?;
        let path = entry.path()?.into_owned();
        let path = path.to_string_lossy();
        process_entry(&mut stats, entry, &mut elf_buffer)
            .map_err(|e| format!("{}: {}", path, e))?;
    }
    Ok(stats)
}

fn process_entry(
    stats: &mut OpcodeStats,
    mut entry: Entry<'_, Stdin>,
    elf_buffer: &mut Vec<u8>,
) -> GenericResult<()> {
    elf_buffer.clear();
    entry.read_to_end(elf_buffer)?;
    stats.add_assign(OpcodeStats::from_program(&elf_buffer)?);
    Ok(())
}

#[derive(Default, Clone, Copy)]
struct OpcodeStat {
    num_programs: usize,
    num_instructions: usize,
}

impl AddAssign for OpcodeStat {
    fn add_assign(&mut self, rhs: Self) {
        self.num_programs += rhs.num_programs;
        self.num_instructions += rhs.num_instructions;
    }
}

struct OpcodeStats([OpcodeStat; 0x100]);

impl Default for OpcodeStats {
    fn default() -> Self {
        Self([OpcodeStat::default(); 0x100])
    }
}

impl AddAssign for OpcodeStats {
    fn add_assign(&mut self, rhs: Self) {
        for i in 0..0x100 {
            self.0[i] += rhs.0[i];
        }
    }
}

impl OpcodeStats {
    fn from_program(elf_bytes: &[u8]) -> GenericResult<Self> {
        let config = Config {
            reject_broken_elfs: false,
            ..Config::default()
        };
        let program = Executable::<FakeEbpfError, FakeInstructionMeter>::from_elf(
            elf_bytes,
            config,
            SyscallRegistry::default(),
        )?;
        let (_, mut text_bytes) = program.get_text_bytes();
        let mut stats = OpcodeStats::default();
        while !text_bytes.is_empty() {
            let insn = get_insn(text_bytes, 0);
            let op_size = if insn.opc != ebpf::LD_DW_IMM { 8 } else { 16 };
            text_bytes = &text_bytes[op_size..];

            let stat = &mut stats.0[insn.opc as usize];
            stat.num_programs |= 1;
            stat.num_instructions += 1;
        }
        Ok(stats)
    }

    fn write_csv<W: Write>(&self, writer: W) -> GenericResult<()> {
        let mut writer = csv::Writer::from_writer(writer);
        writer.write_record(&["opcode", "mnemonic", "num_programs", "num_insns"])?;
        for (opc, stat) in self.0.iter().enumerate() {
            if stat.num_instructions == 0 {
                continue;
            }
            let opc = opc as u8;
            let opcode_num = format!("0x{:02x}", opc);
            let num_programs = stat.num_programs.to_string();
            let num_instructions = stat.num_instructions.to_string();
            writer.write_record(&[
                &opcode_num,
                opcode_mnemonic(opc),
                &num_programs,
                &num_instructions,
            ])?;
        }
        Ok(())
    }
}

struct FakeInstructionMeter();

impl InstructionMeter for FakeInstructionMeter {
    fn consume(&mut self, _amount: u64) {}

    fn get_remaining(&self) -> u64 {
        100
    }
}

struct FakeEbpfError();

impl Error for FakeEbpfError {}

impl Debug for FakeEbpfError {
    fn fmt(&self, _f: &mut Formatter<'_>) -> std::fmt::Result {
        Ok(())
    }
}

impl Display for FakeEbpfError {
    fn fmt(&self, _f: &mut Formatter<'_>) -> std::fmt::Result {
        Ok(())
    }
}

impl UserDefinedError for FakeEbpfError {}

fn opcode_mnemonic(opc: u8) -> &'static str {
    match opc {
        ebpf::LD_ABS_B => "LD_ABS_B",
        ebpf::LD_ABS_H => "LD_ABS_H",
        ebpf::LD_ABS_W => "LD_ABS_W",
        ebpf::LD_ABS_DW => "LD_ABS_DW",
        ebpf::LD_IND_B => "LD_IND_B",
        ebpf::LD_IND_H => "LD_IND_H",
        ebpf::LD_IND_W => "LD_IND_W",
        ebpf::LD_IND_DW => "LD_IND_DW",
        ebpf::LD_DW_IMM => "LD_DW_IMM",
        ebpf::LD_B_REG => "LD_B_REG",
        ebpf::LD_H_REG => "LD_H_REG",
        ebpf::LD_W_REG => "LD_W_REG",
        ebpf::LD_DW_REG => "LD_DW_REG",
        ebpf::ST_B_IMM => "ST_B_IMM",
        ebpf::ST_H_IMM => "ST_H_IMM",
        ebpf::ST_W_IMM => "ST_W_IMM",
        ebpf::ST_DW_IMM => "ST_DW_IMM",
        ebpf::ST_B_REG => "ST_B_REG",
        ebpf::ST_H_REG => "ST_H_REG",
        ebpf::ST_W_REG => "ST_W_REG",
        ebpf::ST_DW_REG => "ST_DW_REG",
        ebpf::ST_W_XADD => "ST_W_XADD",
        ebpf::ST_DW_XADD => "ST_DW_XADD",
        ebpf::ADD32_IMM => "ADD32_IMM",
        ebpf::ADD32_REG => "ADD32_REG",
        ebpf::SUB32_IMM => "SUB32_IMM",
        ebpf::SUB32_REG => "SUB32_REG",
        ebpf::MUL32_IMM => "MUL32_IMM",
        ebpf::MUL32_REG => "MUL32_REG",
        ebpf::DIV32_IMM => "DIV32_IMM",
        ebpf::DIV32_REG => "DIV32_REG",
        ebpf::OR32_IMM => "OR32_IMM",
        ebpf::OR32_REG => "OR32_REG",
        ebpf::AND32_IMM => "AND32_IMM",
        ebpf::AND32_REG => "AND32_REG",
        ebpf::LSH32_IMM => "LSH32_IMM",
        ebpf::LSH32_REG => "LSH32_REG",
        ebpf::RSH32_IMM => "RSH32_IMM",
        ebpf::RSH32_REG => "RSH32_REG",
        ebpf::NEG32 => "NEG32",
        ebpf::MOD32_IMM => "MOD32_IMM",
        ebpf::MOD32_REG => "MOD32_REG",
        ebpf::XOR32_IMM => "XOR32_IMM",
        ebpf::XOR32_REG => "XOR32_REG",
        ebpf::MOV32_IMM => "MOV32_IMM",
        ebpf::MOV32_REG => "MOV32_REG",
        ebpf::ARSH32_IMM => "ARSH32_IMM",
        ebpf::ARSH32_REG => "ARSH32_REG",
        ebpf::SDIV32_IMM => "SDIV32_IMM",
        ebpf::SDIV32_REG => "SDIV32_REG",
        ebpf::LE => "LE",
        ebpf::BE => "BE",
        ebpf::ADD64_IMM => "ADD64_IMM",
        ebpf::ADD64_REG => "ADD64_REG",
        ebpf::SUB64_IMM => "SUB64_IMM",
        ebpf::SUB64_REG => "SUB64_REG",
        ebpf::MUL64_IMM => "MUL64_IMM",
        ebpf::MUL64_REG => "MUL64_REG",
        ebpf::DIV64_IMM => "DIV64_IMM",
        ebpf::DIV64_REG => "DIV64_REG",
        ebpf::OR64_IMM => "OR64_IMM",
        ebpf::OR64_REG => "OR64_REG",
        ebpf::AND64_IMM => "AND64_IMM",
        ebpf::AND64_REG => "AND64_REG",
        ebpf::LSH64_IMM => "LSH64_IMM",
        ebpf::LSH64_REG => "LSH64_REG",
        ebpf::RSH64_IMM => "RSH64_IMM",
        ebpf::RSH64_REG => "RSH64_REG",
        ebpf::NEG64 => "NEG64",
        ebpf::MOD64_IMM => "MOD64_IMM",
        ebpf::MOD64_REG => "MOD64_REG",
        ebpf::XOR64_IMM => "XOR64_IMM",
        ebpf::XOR64_REG => "XOR64_REG",
        ebpf::MOV64_IMM => "MOV64_IMM",
        ebpf::MOV64_REG => "MOV64_REG",
        ebpf::ARSH64_IMM => "ARSH64_IMM",
        ebpf::ARSH64_REG => "ARSH64_REG",
        ebpf::SDIV64_IMM => "SDIV64_IMM",
        ebpf::SDIV64_REG => "SDIV64_REG",
        ebpf::JA => "JA",
        ebpf::JEQ_IMM => "JEQ_IMM",
        ebpf::JEQ_REG => "JEQ_REG",
        ebpf::JGT_IMM => "JGT_IMM",
        ebpf::JGT_REG => "JGT_REG",
        ebpf::JGE_IMM => "JGE_IMM",
        ebpf::JGE_REG => "JGE_REG",
        ebpf::JLT_IMM => "JLT_IMM",
        ebpf::JLT_REG => "JLT_REG",
        ebpf::JLE_IMM => "JLE_IMM",
        ebpf::JLE_REG => "JLE_REG",
        ebpf::JSET_IMM => "JSET_IMM",
        ebpf::JSET_REG => "JSET_REG",
        ebpf::JNE_IMM => "JNE_IMM",
        ebpf::JNE_REG => "JNE_REG",
        ebpf::JSGT_IMM => "JSGT_IMM",
        ebpf::JSGT_REG => "JSGT_REG",
        ebpf::JSGE_IMM => "JSGE_IMM",
        ebpf::JSGE_REG => "JSGE_REG",
        ebpf::JSLT_IMM => "JSLT_IMM",
        ebpf::JSLT_REG => "JSLT_REG",
        ebpf::JSLE_IMM => "JSLE_IMM",
        ebpf::JSLE_REG => "JSLE_REG",
        ebpf::CALL_IMM => "CALL_IMM",
        ebpf::CALL_REG => "CALL_REG",
        ebpf::EXIT => "EXIT",
        _ => "invalid",
    }
}
