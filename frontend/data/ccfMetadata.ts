// CCF spending metadata — manually curated from the CCF Reporting spreadsheet.
// Keyed by proposal URL for uniqueness; provides human-readable labels.
// Source: https://docs.google.com/spreadsheets/d/.../Qubic CCF Reporting

export interface CcfSpendingMeta {
  recipient: string
  category: CcfCategory
  subcategory: string
  description: string
}

export type CcfCategory =
  | 'Tech'
  | 'Marketing'
  | 'QCS'
  | 'Ecosystem'
  | 'Community'
  | 'Operations'
  | 'Audits'
  | 'Scientist'

export const CCF_CATEGORY_COLORS: Record<CcfCategory, string> = {
  Tech: 'rgba(108, 140, 204, 0.8)',       // blue
  Marketing: 'rgba(240, 184, 90, 0.8)',     // amber
  QCS: 'rgba(102, 187, 154, 0.8)',          // green
  Ecosystem: 'rgba(169, 142, 218, 0.8)',    // purple
  Community: 'rgba(80, 190, 210, 0.8)',     // cyan
  Operations: 'rgba(236, 120, 170, 0.8)',   // pink
  Audits: 'rgba(229, 115, 115, 0.8)',       // red
  Scientist: 'rgba(140, 200, 80, 0.8)',     // lime
}

// Address → default recipient name (fallback when URL not matched)
export const CCF_ADDRESS_RECIPIENTS: Record<string, string> = {
  AVXAJKOXPJJYPGKJSRAOSSCOXSNAYLQZPKKUFBDZFFHCYZPBROWKFGRCQCUL: 'wolf',
  JXGMJLWEGFFORBQNEKGPIIZMQNYBNJKJSNWYJSAXDDCZCEZMQUBNGFHFWTMH: 'crypdro',
  TKUWWSNBAEGWJHQJDFLGQHJJCJBAXBSQMQAZJJDYXEPBVBBLIQANJTIDXMQH: 'QCS',
  XQCLNHCEHTKQZDBAHJFVVTRMWFACMAZOBAEDQHEITGGEWZDIBRAIYWPGEONG: 'Joetom',
  EXCEXRFCGCLFRAMXIRLDUAENACKDVQPIZIOJRWTQRCMYWFHFYYFDBQXFINMO: 'Alber',
  YPTIWNXAGOHUOGGBGXNDIGZBVMKCQFBPUVBKDIWGBEKVGIUZVELXAUSCJFEJ: 'KOL Partnership',
  LVRGSAJQRAFELGGAHVAGJLKCGUDDOAEVSDEJAAEGNEVJWQRWDPHOBYHFCJCK: 'Talentnodes',
  STRATGYZMPOAFDMOYMFPFIPWAVGCBPFODBKUCELDQANLSDOHTZYRZDEBGALE: 'Talentnodes',
  COMMUNJOHCBSTFQKYBCZQGXIPQADSJRODNZKKWBAEGVMZFATRTROETTCRVAI: 'El_Clip',
  MARKETVWSHMHFFNKEUSIPLIDAYYAWDYJXYQTIBPVOCEZYLYXHBPYYKQAKLJD: 'Jen_King',
  ECOSYSIWDJKGCGFUKJDKAELJXGVCBHKOWNULFIRGLEGPBDHRIFWDJFUCTRZE: 'Alber',
  PUFZSFKIPJCUOACGETEMFHBVCALAZJSSQFIDYTMHZBZQDRAJXHXIKPVFRBZE: 'Kriptolojik',
  VIOXIFANHGOUACKMYWNBCZQCQMZCLNMDRPTYIAVQZFFEAKNGYFIHDCWCYWMF: 'Raziah',
  BIALNVSDAWDXUCOVZALXDHDTONNBLVVSPVMWGKUTLCAPNUQMCKJANYFCLMZM: 'RyanBabel',
  TQLNOTPKDPLINBPALQHAOJVWHXSBFXTNGEVOVDSNNBGAULUVYGIDYDLFBDDG: 'retrodrive',
  RUBICDEVRDOILARXMHZCUOGLEEKBKUBOEIKCIHDJVAPAWSTDASIZMRGFIVBL: 'matthew',
  VJFQSSLNJMHQRDJTUPVCIWAFSTHCOTCLEQVRNOECFGBOAUOQMEXMLLQBCTGK: 'XXODUX',
  ZYAGKBKGHGAFUCWYDLMTWIGQAYODVDJTCACSHSBYTERJQLPHEVXGJRNAIGNK: 'defimomma',
  MLPJCFNCQIZHGDRSCQIINRYVFQHDDFLDWJGNRRUBZDWSGCRDOJZNDZIHXBEA: 'QubicScience',
}

// Proposal URL substring → full metadata.
// We match by substring so that different commit hashes in GitHub URLs still resolve.
function urlKey(url: string): string {
  // Strip commit hash from GitHub blob URLs for stable matching
  // e.g. /blob/290f9e7d.../file.md → /blob/.../file.md
  return url.replace(/\/blob\/[a-f0-9]{7,40}\//, '/blob/*/').toLowerCase()
}

const RAW_METADATA: Array<{ urlMatch: string } & CcfSpendingMeta> = [
  { urlMatch: 'qvault-smart-contract', recipient: 'wolf', category: 'Tech', subcategory: 'QVAULT', description: 'QVAULT Smart Contract Development' },
  { urlMatch: 'ledger-wallet-integration', recipient: 'crypdro', category: 'Tech', subcategory: 'Ledger', description: 'Ledger Hardware Wallet App' },
  { urlMatch: 'qcs-revised-budget', recipient: 'QCS', category: 'QCS', subcategory: 'General', description: 'Qubic Core Services - Base Budget' },
  { urlMatch: '2024-11-14-tech-funding', recipient: 'Joetom', category: 'Tech', subcategory: 'Core', description: 'Qubic Core Development' },
  { urlMatch: 'qcs-decemberbudgetrequest', recipient: 'QCS', category: 'QCS', subcategory: 'General', description: 'QCS - December Budget' },
  { urlMatch: 'exchangelistingproposal', recipient: 'Alber', category: 'Ecosystem', subcategory: 'Exchanges', description: 'Exchange Listings' },
  { urlMatch: 'kol-partnership-proposal', recipient: 'Unknown', category: 'Marketing', subcategory: 'Social Media', description: 'KOL Partnership Marketing Campaign' },
  { urlMatch: 'qcs-budget-q12025', recipient: 'QCS', category: 'QCS', subcategory: 'General', description: 'QCS - Q1 2025 Budget' },
  { urlMatch: 'ambassadorprogram', recipient: 'Talentnodes', category: 'Community', subcategory: 'Ambassador', description: 'Ambassador Program' },
  { urlMatch: 'qcs%20operations%20ccf%20proposal', recipient: 'Talentnodes', category: 'Operations', subcategory: 'General', description: 'QCS Strategy, Finance & Operations' },
  { urlMatch: 'qcs operations ccf proposal', recipient: 'Talentnodes', category: 'Operations', subcategory: 'General', description: 'QCS Strategy, Finance & Operations' },
  { urlMatch: '2025-04-12-tech-funding-q2', recipient: 'Joetom', category: 'Tech', subcategory: 'Core', description: 'Qubic Core Development Q2' },
  { urlMatch: 'community%20management%20ccf%20proposal%20(april-may)', recipient: 'El_Clip', category: 'Community', subcategory: 'General', description: 'Community Management (Apr-May)' },
  { urlMatch: 'community management ccf proposal (april-may)', recipient: 'El_Clip', category: 'Community', subcategory: 'General', description: 'Community Management (Apr-May)' },
  { urlMatch: 'qcs%20marketing%20budget%20proposal%20may-june', recipient: 'Jen_King', category: 'Marketing', subcategory: 'General', description: 'Marketing Budget (May-June)' },
  { urlMatch: 'qcs marketing budget proposal may-june', recipient: 'Jen_King', category: 'Marketing', subcategory: 'General', description: 'Marketing Budget (May-June)' },
  { urlMatch: 'qcs%20marketing%20ccf%20proposal%20april', recipient: 'Jen_King', category: 'Marketing', subcategory: 'General', description: 'Marketing (April)' },
  { urlMatch: 'qcs marketing ccf proposal april', recipient: 'Jen_King', category: 'Marketing', subcategory: 'General', description: 'Marketing (April)' },
  { urlMatch: 'qubic%20ecosystem%20services%20ccf%20proposal%20(may', recipient: 'Alber', category: 'Ecosystem', subcategory: 'General', description: 'Ecosystem Services (May-Jun)' },
  { urlMatch: 'qubic ecosystem services ccf proposal (may', recipient: 'Alber', category: 'Ecosystem', subcategory: 'General', description: 'Ecosystem Services (May-Jun)' },
  { urlMatch: 'qubicaiforgood', recipient: 'DavidVivancos', category: 'Ecosystem', subcategory: 'Events', description: 'AI for Good Event' },
  { urlMatch: 'qubicibw2025', recipient: 'Kriptolojik', category: 'Marketing', subcategory: 'Events', description: 'Istanbul Blockchain Week 2025' },
  { urlMatch: 'raise%20summit%20hackathon', recipient: 'Raziah', category: 'Marketing', subcategory: 'Hackathon', description: 'RAISE Summit Hackathon 2025' },
  { urlMatch: 'raise summit hackathon', recipient: 'Raziah', category: 'Marketing', subcategory: 'Hackathon', description: 'RAISE Summit Hackathon 2025' },
  { urlMatch: 'ambassador%20program%20s03', recipient: 'Talentnodes', category: 'Community', subcategory: 'Ambassador', description: 'Ambassador Program S03' },
  { urlMatch: 'ambassador program s03', recipient: 'Talentnodes', category: 'Community', subcategory: 'Ambassador', description: 'Ambassador Program S03' },
  { urlMatch: 'cointribune', recipient: 'Raziah', category: 'Marketing', subcategory: 'General', description: 'Qubic France - Cointribune' },
  { urlMatch: 'exchange%20listing%20proposal', recipient: 'Alber', category: 'Ecosystem', subcategory: 'Exchanges', description: 'Exchange Listing (Top-15)' },
  { urlMatch: 'exchange listing proposal', recipient: 'Alber', category: 'Ecosystem', subcategory: 'Exchanges', description: 'Exchange Listing (Top-15)' },
  { urlMatch: 'ryan%20babel', recipient: 'RyanBabel', category: 'Marketing', subcategory: 'Social Media', description: 'Ryan Babel Partnership' },
  { urlMatch: 'ryan babel', recipient: 'RyanBabel', category: 'Marketing', subcategory: 'Social Media', description: 'Ryan Babel Partnership' },
  { urlMatch: 'marketing%20revival', recipient: 'retrodrive', category: 'Marketing', subcategory: 'General', description: 'Marketing Revival' },
  { urlMatch: 'marketing revival', recipient: 'retrodrive', category: 'Marketing', subcategory: 'General', description: 'Marketing Revival' },
  { urlMatch: 'community-management-ccf-proposal-june-july', recipient: 'El_Clip', category: 'Community', subcategory: 'General', description: 'Community Management (Jun-Jul)' },
  { urlMatch: 'rubicproposal', recipient: 'matthew', category: 'Tech', subcategory: 'Rubic', description: 'Rubic Project' },
  { urlMatch: 'web3lagos', recipient: 'XXODUX', category: 'Marketing', subcategory: 'Events', description: 'Web3Lagos Conference 2025' },
  { urlMatch: 'core%20services%20ccf%20proposal%20-%20strategy', recipient: 'Talentnodes', category: 'Operations', subcategory: 'General', description: 'Core Services: Strategy & Operations' },
  { urlMatch: 'core services ccf proposal - strategy', recipient: 'Talentnodes', category: 'Operations', subcategory: 'General', description: 'Core Services: Strategy & Operations' },
  { urlMatch: 'rareevo', recipient: 'Unknown', category: 'Marketing', subcategory: 'Events', description: 'RareEvo Event' },
  { urlMatch: 'ecosystem%20services%20ccf%20proposal%20(august', recipient: 'Alber', category: 'Ecosystem', subcategory: 'General', description: 'Ecosystem Services (Aug-Oct)' },
  { urlMatch: 'ecosystem services ccf proposal (august', recipient: 'Alber', category: 'Ecosystem', subcategory: 'General', description: 'Ecosystem Services (Aug-Oct)' },
  { urlMatch: 'ccf%20proposal%20%e2%80%93%20qubic%20marketing%3a%20ccf%20funding', recipient: 'Jen_King', category: 'Marketing', subcategory: 'General', description: 'Qubic Marketing Funding' },
  { urlMatch: 'qubic marketing: ccf funding', recipient: 'Jen_King', category: 'Marketing', subcategory: 'General', description: 'Qubic Marketing Funding' },
  { urlMatch: 'milly', recipient: 'Milly', category: 'Marketing', subcategory: 'Social Media', description: 'Milly x Qubic Ecosystem Amplification' },
  { urlMatch: 'penetration%20test', recipient: 'sally', category: 'Audits', subcategory: 'Wallet', description: 'iOS/Android Wallet Penetration Test' },
  { urlMatch: 'penetration test', recipient: 'sally', category: 'Audits', subcategory: 'Wallet', description: 'iOS/Android Wallet Penetration Test' },
  { urlMatch: 'community%20tech%20contributors', recipient: 'El_Clip', category: 'Tech', subcategory: 'Contributions', description: 'Community Tech Contributors' },
  { urlMatch: 'community tech contributors', recipient: 'El_Clip', category: 'Tech', subcategory: 'Contributions', description: 'Community Tech Contributors' },
  { urlMatch: 'community%20marketing%20contributors', recipient: 'El_Clip', category: 'Marketing', subcategory: 'Contributions', description: 'Community Marketing Contributors' },
  { urlMatch: 'community marketing contributors', recipient: 'El_Clip', category: 'Marketing', subcategory: 'Contributions', description: 'Community Marketing Contributors' },
  { urlMatch: 'community%20contributors%20ccf', recipient: 'El_Clip', category: 'Community', subcategory: 'Contributions', description: 'Community Contributors' },
  { urlMatch: 'community contributors ccf', recipient: 'El_Clip', category: 'Community', subcategory: 'Contributions', description: 'Community Contributors' },
  { urlMatch: 'exchange%20spot%20listing%20proposal%20for%20top-10', recipient: 'Alber', category: 'Ecosystem', subcategory: 'Exchanges', description: 'Exchange Spot Listing (Top-10)' },
  { urlMatch: 'exchange spot listing proposal for top-10', recipient: 'Alber', category: 'Ecosystem', subcategory: 'Exchanges', description: 'Exchange Spot Listing (Top-10)' },
  { urlMatch: 'great%20crypto%20escape', recipient: 'The Great Crypto Escape', category: 'Marketing', subcategory: 'Social Media', description: 'Video Media Campaign' },
  { urlMatch: 'great crypto escape', recipient: 'The Great Crypto Escape', category: 'Marketing', subcategory: 'Social Media', description: 'Video Media Campaign' },
  { urlMatch: 'exchange%20spot%20listing%20proposal%20in%20the%20us', recipient: 'Alber', category: 'Ecosystem', subcategory: 'Exchanges', description: 'US Exchange Spot Listing' },
  { urlMatch: 'exchange spot listing proposal in the us', recipient: 'Alber', category: 'Ecosystem', subcategory: 'Exchanges', description: 'US Exchange Spot Listing' },
  { urlMatch: 'exchange%20spot%20listing%20proposal%20for%20top-5', recipient: 'Alber', category: 'Ecosystem', subcategory: 'Exchanges', description: 'Exchange Spot Listing (Top-5 Tier 1)' },
  { urlMatch: 'exchange spot listing proposal for top-5', recipient: 'Alber', category: 'Ecosystem', subcategory: 'Exchanges', description: 'Exchange Spot Listing (Top-5 Tier 1)' },
  { urlMatch: 'q-stream', recipient: 'Revo', category: 'Marketing', subcategory: 'Social Media', description: 'Q-Stream' },
  { urlMatch: 'qswap', recipient: 'Unknown', category: 'Tech', subcategory: 'QSWAP', description: 'QSWAP SC Functions' },
  { urlMatch: 'community%20management%20ccf%20proposal%20for%20september', recipient: 'El_Clip', category: 'Community', subcategory: 'Community', description: 'Community Management (Sep-Nov)' },
  { urlMatch: 'community management ccf proposal for september', recipient: 'El_Clip', category: 'Community', subcategory: 'Community', description: 'Community Management (Sep-Nov)' },
  { urlMatch: 'ambassador%20program%20so4', recipient: 'Unknown', category: 'Community', subcategory: 'Ambassador', description: 'Ambassador Program SO4' },
  { urlMatch: 'ambassador program so4', recipient: 'Unknown', category: 'Community', subcategory: 'Ambassador', description: 'Ambassador Program SO4' },
  { urlMatch: 'shanghai%20global%20blockchain', recipient: 'Zhangxingyuan', category: 'Marketing', subcategory: 'Events', description: 'Shanghai Global Blockchain Week' },
  { urlMatch: 'shanghai global blockchain', recipient: 'Zhangxingyuan', category: 'Marketing', subcategory: 'Events', description: 'Shanghai Global Blockchain Week' },
  { urlMatch: 'qubic%20socials%20content%20and%20coordination', recipient: 'retrodrive', category: 'Marketing', subcategory: 'General', description: 'Qubic Marketing Funding' },
  { urlMatch: 'qubic socials content and coordination', recipient: 'retrodrive', category: 'Marketing', subcategory: 'General', description: 'Qubic Marketing Funding' },
  { urlMatch: '2025-10-tech-core-funding', recipient: 'Joetom', category: 'Tech', subcategory: 'Core', description: 'Qubic Core Development' },
  { urlMatch: 'tech-core-funding', recipient: 'Joetom', category: 'Tech', subcategory: 'Core', description: 'Qubic Core Development' },
  { urlMatch: 'ecosystem%20services%20proposal%20%e2%80%93%20ai%20transition', recipient: 'Alber', category: 'Ecosystem', subcategory: 'Core', description: 'Ecosystem Services - AI Transition Phase' },
  { urlMatch: 'ecosystem services proposal', recipient: 'Alber', category: 'Ecosystem', subcategory: 'Core', description: 'Ecosystem Services' },
  { urlMatch: 'qubic%20marketing%20proposal', recipient: 'defimomma', category: 'Marketing', subcategory: 'Core', description: 'Qubic Marketing Proposal' },
  { urlMatch: 'final qubic marketing proposal', recipient: 'defimomma', category: 'Marketing', subcategory: 'Core', description: 'Qubic Marketing Proposal' },
  { urlMatch: 'qubicscience2026', recipient: 'QubicScience', category: 'Scientist', subcategory: 'Core', description: 'AGI Research 2026 First Trimester' },
]

/**
 * Look up metadata for a CCF spending by its proposal URL.
 * Falls back to address-based recipient name if no URL match.
 */
export function lookupCcfMeta(url: string | null | undefined, address?: string): CcfSpendingMeta | null {
  if (url) {
    const lower = url.toLowerCase()
    for (const entry of RAW_METADATA) {
      if (lower.includes(entry.urlMatch.toLowerCase())) {
        return {
          recipient: entry.recipient,
          category: entry.category,
          subcategory: entry.subcategory,
          description: entry.description,
        }
      }
    }
  }

  // Fallback: address-based recipient name only
  if (address) {
    const recipient = CCF_ADDRESS_RECIPIENTS[address]
    if (recipient) {
      return { recipient, category: 'Tech', subcategory: 'General', description: '' }
    }
  }

  return null
}
