package main

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/joho/godotenv"
	_ "github.com/lib/pq"
	"golang.org/x/exp/rand"
	"golang.org/x/net/context"
	"google.golang.org/api/option"
)

var (
	ctx      = context.Background()
	logger   *Logger
	db       *sql.DB
	bqClient *bigquery.Client
)

// Logger struct to encapsulate the standard logger
type Logger struct {
	logger *log.Logger
}

// LogInfo writes an informational message
func (l *Logger) LogInfo(format string, args ...interface{}) {
	l.logger.Printf("[INFO] "+format, args...)
}

// LogWarn writes a warning message
func (l *Logger) LogWarn(format string, args ...interface{}) {
	l.logger.Printf("[WARN] "+format, args...)
}

// LogError writes an error message
func (l *Logger) LogError(format string, args ...interface{}) {
	l.logger.Printf("[ERROR] "+format, args...)
}

// LogFatal writes an error message and then exits the application
func (l *Logger) LogFatal(format string, args ...interface{}) {
	l.logger.Fatalf("[FATAL] "+format, args...)
}

// Initialize Redis and SQL clients
func init() {
	// Init logger
	logger = &Logger{
		logger: log.New(os.Stdout, "", log.LstdFlags),
	}

	var err error

	// Load environment variables from .env file
	if err = godotenv.Load(); err != nil {
		logger.LogFatal("[SYSTEM] Error loading .env file")
	}

	db, err = sql.Open("postgres", os.Getenv("POSTGRES_DSN"))
	if err != nil {
		logger.LogFatal("[SYSTEM] Failed to connect to PostgreSQL: %v", err)
	}
	logger.LogInfo("[SYSTEM] Connected to PostgreSQL")

	bqClient, err = bigquery.NewClient(ctx, os.Getenv("GCP_PROJECT_ID"), option.WithCredentialsFile(os.Getenv("GCP_CREDENTIALS_FILE")))
	if err != nil {
		logger.LogFatal("[SYSTEM] Failed to connect to BigQuery: %v", err)
	}
	logger.LogInfo("[SYSTEM] Connected to BigQuery")
}

func main() {
	const numRows = 100

	// Define the BigQuery table
	table := bqClient.Dataset(os.Getenv("ENV") + "_weather").Table("page")

	// Helper functions
	randomPublicationDate := func() time.Time {
		now := time.Now()
		start := now.AddDate(0, -1, 0) // One month ago
		return start.Add(time.Duration(rand.Int63n(now.Sub(start).Nanoseconds())) * time.Nanosecond)
	}

	randomTitle := func(i int) string {
		return fmt.Sprintf("Article Title %d", i)
	}

	randomDescription := func(i int) string {
		return fmt.Sprintf("This is a description for article %d", i)
	}

	randomSection := func(i int) string {
		sections := []string{"Technology", "Science", "Health", "Travel", "Lifestyle"}
		return sections[i%len(sections)] // Rotate through sections
	}

	randomImage := func(i int) *string {
		if rand.Intn(2) == 0 {
			return nil
		}
		image := fmt.Sprintf("https://www.example.com/images/image-%d.jpg", i)
		return &image
	}

	randomIsPaid := func() bool {
		return rand.Intn(2) == 0 // Randomly returns true or false
	}

	// Randomly generates a long content for an article in French
	randomContent := func(i int) string {
		phrases := []string{
			"Dans un petit village pittoresque, les ruelles pavées sont bordées de maisons colorées, où chaque fenêtre raconte une histoire.",
			"Les enfants jouent dans les champs fleuris, riant aux éclats tandis que les papillons dansent autour d'eux sous un ciel bleu azur.",
			"Chaque matin, la boulangerie locale embaume l'air de l'odeur du pain frais, attirant les habitants en quête de douceurs.",
			"Les arbres majestueux se dressent fièrement, leurs feuilles bruissant doucement au gré du vent, offrant une ombre bienveillante aux promeneurs.",
			"Lors d'une promenade en forêt, les rayons du soleil percent le feuillage, créant des jeux de lumière qui enchantent les yeux émerveillés.",
			"Le murmure d'un ruisseau voisin accompagne les pensées profondes lors de moments de solitude et de réflexion sur la vie.",
			"Chaque été, le village organise une fête où rires et musique résonnent, rassemblant la communauté autour de danses traditionnelles.",
			"Les marchés locaux débordent de couleurs vives, avec des étals remplis de fruits et légumes frais, témoignant de la richesse des terres environnantes.",
			"Les souvenirs d'enfance sont souvent ancrés dans des lieux familiers, où chaque coin rappelle des moments de joie partagée avec des amis.",
			"Les soirées d'hiver sont souvent passées autour d'un bon feu de cheminée, avec des livres captivants et des tasses de chocolat chaud.",
			"La montagne, majestueuse et silencieuse, offre des panoramas à couper le souffle qui laissent les randonneurs sans voix.",
			"Les nuits étoilées au bord du lac sont parfaites pour contempler l'infini, où chaque étoile semble raconter une légende oubliée.",
			"Les festivals de musique rassemblent des artistes talentueux et des spectateurs passionnés, créant une atmosphère électrique de joie et d'unité.",
			"Les voyages en train à travers des paysages variés révèlent la beauté cachée des régions, offrant des moments de contemplation inoubliables.",
			"Les fleurs sauvages qui poussent le long des chemins évoquent des souvenirs de printemps, apportant des touches de couleur dans le quotidien.",
			"À chaque lever de soleil, le monde semble renaître, offrant une nouvelle chance de vivre pleinement chaque instant.",
			"Les recettes de famille, transmises de génération en génération, sont une véritable célébration de l'héritage culinaire et des traditions.",
			"Les livres sont des passeports vers des mondes imaginaires, permettant d'échapper à la réalité et d'explorer des histoires fascinantes.",
			"Les soirées passées entre amis, à partager des rires et des histoires, sont souvent les plus mémorables et les plus précieuses.",
			"Le jardin potager regorge de légumes croquants, offrant une véritable source de fierté pour ceux qui aiment cultiver la terre.",
			"Les balades en vélo à travers les champs dorés de blé créent un sentiment de liberté et de connexion avec la nature.",
			"Les contes et légendes racontés au coin du feu laissent une empreinte durable, transmettant des valeurs et des leçons importantes.",
			"Les oiseaux migrateurs, avec leurs chants mélodieux, annoncent le changement des saisons et l'arrivée de la beauté printanière.",
			"Les activités artistiques, qu'il s'agisse de peinture ou de sculpture, permettent d'exprimer des émotions et de créer des œuvres uniques.",
			"La cuisine est un art à part entière, où chaque plat est préparé avec soin et passion, célébrant les saveurs et les arômes.",
			"Les promenades en bord de mer, les pieds dans le sable chaud, sont une invitation à la rêverie et à la détente.",
			"Les rituels du matin, comme boire un café en regardant le lever du soleil, apportent une sérénité précieuse au début de la journée.",
			"Les vieux châteaux, témoins d'un passé glorieux, transportent les visiteurs dans l'histoire, éveillant curiosité et admiration.",
			"Les amis sont comme des étoiles, même si on ne les voit pas toujours, on sait qu'ils sont là, prêts à illuminer notre chemin.",
			"Les émotions ressenties lors d'une première danse sont inoubliables, créant des souvenirs gravés à jamais dans nos cœurs.",
			"Les souvenirs de voyages, qu'ils soient lointains ou proches, sont des trésors qui enrichissent notre existence et notre perspective.",
			"Les rivières sinueuses, entourées de verdure, apportent une fraîcheur apaisante et sont souvent le refuge des pêcheurs en quête de tranquillité.",
			"Les sculptures dans les parcs publics ajoutent une touche d'art et de culture, invitant les passants à s'arrêter et à admirer.",
			"Les produits artisanaux, créés avec passion et savoir-faire, sont des cadeaux uniques qui portent en eux l'histoire de leurs créateurs.",
			"Les moments de silence, souvent sous-estimés, sont en réalité des occasions précieuses de se reconnecter avec soi-même.",
			"Les balades en pleine nature, entourées de paysages grandioses, offrent un répit à l'esprit et un souffle nouveau à l'âme.",
			"Les fêtes de fin d'année sont une période magique, où l'on se retrouve en famille, entouré de lumières scintillantes et de douceurs.",
			"Les photographies capturent des instants fugaces, permettant de revivre des souvenirs et d'immortaliser des moments précieux.",
			"Les couchers de soleil, avec leurs couleurs flamboyantes, évoquent souvent des réflexions sur le temps qui passe et la beauté de l'instant présent.",
			"Les événements communautaires, tels que les foires et les festivals, renforcent les liens entre les habitants et célèbrent la diversité.",
			"Les balades en montgolfière offrent une vue imprenable sur le paysage, créant une expérience mémorable et unique.",
			"Les chansons qui résonnent dans nos têtes sont souvent liées à des moments marquants de notre vie, créant des souvenirs indélébiles.",
			"Les vieilles pierres des bâtiments historiques sont les témoins d'une époque révolue, portant en elles les histoires de ceux qui ont vécu là.",
			"Les visites de musées permettent d'explorer l'art sous toutes ses formes et de s'immerger dans des univers fascinants.",
			"Les traditions familiales, qu'elles soient culinaires ou culturelles, créent des liens forts et des souvenirs inoubliables.",
			"Les sourires échangés avec des inconnus apportent une chaleur inattendue et un sentiment d'appartenance à une communauté.",
			"Les balades en canoë sur des lacs tranquilles sont l'occasion de se ressourcer et de profiter de la beauté de la nature.",
			"Les éclats de rire d'enfants résonnent dans les parcs, remplissant l'air d'une énergie joyeuse et contagieuse.",
			"Les anciens livres, avec leur odeur de papier jauni, offrent une plongée dans des histoires intemporelles et des savoirs anciens.",
			"Les randonnées en montagne, bien que parfois éprouvantes, récompensent les efforts par des panoramas à couper le souffle.",
			"Les pique-niques sur l'herbe, entourés de proches, sont des moments de partage et de bonheur simples qui font du bien au cœur.",
			"Les villes anciennes, avec leurs ruelles étroites et pavées, invitent à la flânerie et à la découverte de trésors cachés.",
			"Les promenades en forêt, avec le chant des oiseaux en fond sonore, apportent une sérénité et un apaisement bienfaisants.",
			"Les artisans locaux, en perpétuant des savoir-faire traditionnels, préservent la culture et l'identité de leur région.",
			"Les souvenirs d'été sont souvent liés aux rires, aux baignades en mer et aux soirées à contempler les étoiles.",
			"Les terrasses de café, avec leurs tables ensoleillées, sont des lieux de rencontre privilégiés pour partager un moment convivial.",
			"Les paysages d'automne, avec leurs feuilles dorées et rouges, créent une atmosphère magique et mélancolique.",
			"Les récits de voyages lointains, racontés avec passion, éveillent en nous le désir d'explorer le monde et de découvrir l'inconnu.",
			"Les traditionnelles fêtes de Noël apportent une chaleur particulière, où les familles se rassemblent autour d'un bon repas.",
			"Les jardins fleuris sont des havres de paix, où chaque fleur, chaque parfum nous rappelle la beauté de la nature.",
			"Les grands-mères, avec leurs histoires et leurs recettes, sont souvent des gardiennes de la tradition et de l'amour familial.",
			"Les balades en voiture à travers des paysages pittoresques sont l'occasion de vivre des moments inoubliables et de s'émerveiller.",
			"Les expressions artistiques, qu'elles soient musicales ou visuelles, nous touchent et nous permettent de ressentir des émotions profondes.",
			"Les festivals de danse rassemblent des talents venus de tous horizons, célébrant la diversité et la richesse de la culture.",
			"Les paysages enneigés en hiver, avec leur calme et leur beauté, sont parfaits pour une escapade romantique ou en famille.",
			"Les rencontres imprévues avec des amis de longue date sont souvent source de joie et de nostalgie, ravivant de précieux souvenirs.",
			"Les légendes racontées autour d'un feu de camp éveillent notre imagination et nous connectent à notre histoire collective.",
			"Les saveurs exotiques de la cuisine du monde nous font voyager à travers des cultures différentes, élargissant nos horizons.",
			"Les fleurs qui poussent spontanément dans les champs rappellent que la beauté se trouve souvent là où on s'y attend le moins.",
			"Les moments passés à observer les étoiles sont l'occasion de rêver, de se poser des questions sur notre existence et l'univers.",
			"Les musées d'art contemporain, avec leurs œuvres audacieuses, nous invitent à réfléchir sur notre époque et notre société.",
			"Les promenades sur les quais d'un fleuve offrent des vues magnifiques et sont un excellent moyen de se détendre.",
			"Les traditions de chaque région enrichissent notre patrimoine culturel et nous rappellent la beauté de la diversité humaine.",
			"Les notes de musique qui s'élèvent lors d'un concert créent une atmosphère magique, rassemblant les cœurs autour d'une passion commune.",
			"Les histoires de famille, racontées avec tendresse, renforcent notre identité et nous rappellent d'où nous venons.",
			"Les rituels du coucher, tels que lire une histoire aux enfants, sont des moments de partage précieux qui créent des liens forts.",
			"Les balades en bateau sur des eaux calmes offrent une perspective différente des paysages, rendant chaque instant unique.",
			"Les petits gestes quotidiens, comme offrir un compliment ou aider un voisin, contribuent à créer une atmosphère bienveillante.",
			"Les couchers de soleil au bord de la mer sont des spectacles grandioses qui nous rappellent la beauté de la nature.",
			"Les balades à cheval, au grand air, permettent de se reconnecter avec la nature et de vivre des moments de sérénité.",
			"Les festivals de cinéma rassemblent des passionnés et des créateurs, célébrant la magie du 7ème art et ses histoires émouvantes.",
			"Les souvenirs d'un voyage marquent nos esprits, nous faisant rêver de futures aventures et de découvertes à venir.",
			"Les promenades en automne, lorsque les feuilles tombent, sont une invitation à réfléchir et à apprécier la beauté de chaque saison.",
			"Les rituels de café du matin, partagés avec des proches, apportent une chaleur et une intimité précieuses à nos journées.",
			"Les rivières, en s'écoulant paisiblement, apportent une touche de sérénité et de tranquillité à notre quotidien.",
			"Les villages perchés sur des collines offrent des panoramas à couper le souffle, où chaque vue est un tableau vivant.",
			"Les contes de fées, que nous lisons ou racontons, éveillent notre imagination et nourrissent notre désir d'évasion.",
			"Les traditions culinaires, avec leurs saveurs uniques, sont une manière délicieuse de découvrir une culture et son histoire.",
			"Les couchers de soleil sont des moments magiques qui nous rappellent d'apprécier chaque jour comme un cadeau précieux.",
			"Les balades en vélo sur des pistes cyclables offrent une sensation de liberté et d'exploration, loin du tumulte de la ville.",
			"Les récits de voyages sont souvent remplis d'aventures palpitantes et de rencontres inoubliables qui enrichissent notre vie.",
			"Les soirées d'été, passées à discuter autour d'un feu de camp, sont l'occasion de tisser des liens profonds avec ceux qui nous entourent.",
			"Les traditions de Pâques, avec leurs œufs colorés et leurs chasses aux trésors, apportent joie et convivialité au printemps.",
			"Les livres de voyage sont de véritables fenêtres sur le monde, nous permettant de découvrir des cultures et des paysages différents.",
			"Les danses folkloriques, souvent exécutées lors de festivals, célèbrent les racines et la diversité de chaque communauté.",
			"Les balades sur la plage, au son des vagues, apportent un sentiment de paix et de connexion avec la nature.",
			"Les marchés de Noël, avec leurs lumières scintillantes et leurs odeurs de vin chaud, créent une ambiance festive et chaleureuse.",
			"Les souvenirs de vacances passées, remplis de rires et de découvertes, sont souvent ceux qui nous font sourire des années plus tard.",
			"Les sculptures sur les places publiques ajoutent une dimension artistique à notre quotidien, suscitant curiosité et admiration.",
			"Les levers de soleil sur la montagne sont des spectacles à couper le souffle, marquant le début d'une nouvelle journée pleine de promesses.",
			"Les histoires de la mythologie, riches en enseignements, nous rappellent l'importance des valeurs humaines et de l'harmonie.",
			"Les randonnées en pleine nature, entourées de paysages magnifiques, sont une façon de se ressourcer et de trouver la paix intérieure.",
			"Les soirées de jeux de société entre amis sont souvent des occasions de rire aux éclats et de créer des souvenirs inoubliables.",
			"Les festivals de théâtre, avec leurs performances émouvantes, célèbrent l'art vivant et nous invitent à réfléchir sur la condition humaine.",
			"Les balades en tramway à travers la ville sont une manière agréable de découvrir ses trésors cachés et son histoire.",
			"Les vieilles chansons évoquent souvent des souvenirs d'amour et de nostalgie, nous transportant dans le passé avec tendresse.",
			"Les traditions du Nouvel An, avec leurs feux d'artifice et leurs résolutions, marquent le passage à une nouvelle année pleine d'espoir.",
			"Les promenades en bateau-mouche sur les rivières célèbrent la beauté des villes et offrent une perspective unique sur leur architecture.",
			"Les petits marchés de producteurs locaux sont des trésors cachés où l'on découvre des saveurs authentiques et des produits de qualité.",
			"Les heures passées à observer la nature, qu'il s'agisse de fleurs en fleurs ou d'oiseaux en vol, nourrissent notre âme.",
			"Les spectacles de danse contemporaine nous fascinent par leur créativité et leur capacité à exprimer des émotions complexes.",
			"Les nuits d'été passées à contempler le ciel étoilé sont des moments magiques qui nous connectent à l'univers et à nous-mêmes.",
			"Les rituels du thé de l'après-midi, avec leurs douceurs, apportent une touche de douceur à nos journées bien remplies.",
			"Les courses de bateaux à voile, avec leurs voiles blanches se dressant fièrement contre le ciel bleu, sont un spectacle à ne pas manquer.",
			"Les souvenirs des fêtes d'anniversaire, avec leurs gâteaux et leurs rires, créent des liens indélébiles entre amis et famille.",
			"Les balades en calèche à travers les rues historiques évoquent une époque révolue et apportent une touche de romantisme.",
			"Les petits gestes de gentillesse, comme tenir la porte ouverte ou offrir un sourire, peuvent illuminer la journée de quelqu'un.",
			"Les soirées de cinéma en plein air, avec leurs films classiques et leurs étoiles scintillantes, créent une ambiance conviviale et chaleureuse.",
			"Les ateliers d'artisanat permettent de découvrir des techniques anciennes tout en créant des objets uniques et personnels.",
			"Les promenades nocturnes sous la lune, avec les sons de la nature comme accompagnement, apportent une paix intérieure profonde.",
			"Les histoires de résilience, qu'elles soient personnelles ou collectives, nous inspirent et nous rappellent la force de l'esprit humain.",
			"Les traditions de printemps, avec leurs célébrations de renouveau et de croissance, apportent de l'espoir et de la joie.",
			"Les rivages de la mer, avec leurs vagues déferlantes et leur parfum salin, sont un lieu de ressourcement et de méditation.",
			"Les moments passés à jouer à des jeux de société en famille renforcent les liens et créent des souvenirs inestimables.",
			"Les après-midi pluvieux passés à lire des romans sous une couverture sont des occasions parfaites de se plonger dans d'autres mondes.",
			"Les récits de sagesse, transmis par les ancêtres, nous guident et nous rappellent les leçons importantes de la vie.",
			"Les paysages de montagne, avec leurs cimes enneigées, inspirent souvent des sentiments d'émerveillement et d'humilité.",
			"Les rencontres fortuites, que ce soit dans un café ou lors d'un voyage, peuvent parfois changer le cours de notre vie.",
			"Les veillées passées autour d'un feu de camp sont des moments de partage et de camaraderie qui nourrissent l'âme.",
			"Les traditions du dimanche, comme le repas en famille, renforcent les liens et créent des souvenirs précieux.",
			"Les sons apaisants de la nature, comme le chant des oiseaux ou le bruit d'une rivière, sont une source de réconfort et de paix.",
			"Les explorations urbaines, à la recherche de petits cafés cachés et de galeries d'art, sont des aventures qui éveillent les sens.",
			"Les couchers de soleil sur la plage, baignés de couleurs chaudes, sont des instants magiques qui restent gravés dans nos mémoires.",
			"Les vieilles photographies, pleines de souvenirs et d'histoires, nous rappellent d'où nous venons et nous incitent à nous souvenir.",
			"Les soirées autour d'une table à discuter de tout et de rien renforcent les liens d'amitié et créent des souvenirs durables.",
			"Les promenades dans les parcs, entourés de verdure et de fleurs, sont des occasions de se ressourcer et de se reconnecter avec soi-même.",
			"Les concerts en plein air, avec leur ambiance festive, rassemblent des gens autour de la musique et des émotions partagées.",
			"Les récits de voyages, empreints de découvertes et d'aventures, nourrissent notre curiosité et notre envie d'explorer le monde.",
			"Les petits gestes de gratitude, comme un simple merci, peuvent avoir un impact profond sur notre quotidien et celui des autres.",
			"Les souvenirs des saisons passées, avec leurs couleurs et leurs sensations, nous rappellent la beauté des cycles de la vie.",
		}

		return phrases[i]
	}

	var rows []bigquery.ValueSaver

	// Prepare the SQL insert statement
	stmt, err := db.Prepare(`
		INSERT INTO page (brand, type, url, publication_date, title, description, section, image, is_paid, content) 
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
	`)
	if err != nil {
		logger.LogError("Failed to prepare the SQL statement: %v", err)
		return
	}
	defer stmt.Close()

	// Generate and insert data
	for i := 1; i <= numRows; i++ {
		brand := "test"
		pageType := "article"
		language := "fr_FR"
		url := fmt.Sprintf("https://www.example.com/article-%d.html", i)
		publicationDate := randomPublicationDate()
		title := randomTitle(i)
		description := randomDescription(i)
		section := randomSection(i)
		image := randomImage(i)
		isPaid := randomIsPaid()
		content := randomContent(i) // Generate random content

		// In PostgreSQL, NULL values are represented as nil in Go
		_, err := stmt.Exec(brand, pageType, url, publicationDate, title, description, section, image, isPaid, content)
		if err != nil {
			logger.LogError("Failed to execute the SQL statement: %v", err)
			return
		}

		var imageValue string
		if image != nil {
			imageValue = *image
		}

		rows = append(rows, &bigquery.StructSaver{
			Struct: struct {
				DateTime        time.Time `bigquery:"datetime"`
				Brand           string    `bigquery:"brand"`
				Url             string    `bigquery:"url"`
				Type            string    `bigquery:"type"`
				Language        string    `bigquery:"language"`
				Title           string    `bigquery:"title"`
				Description     string    `bigquery:"description"`
				PublicationDate time.Time `bigquery:"publication_date"`
				Section         string    `bigquery:"section"`
				Image           string    `bigquery:"image"`
				IsPaid          bool      `bigquery:"is_paid"`
				Content         string    `bigquery:"content"`
			}{
				DateTime:        time.Now().UTC(),
				Brand:           brand,
				Url:             url,
				Type:            pageType,
				Language:        language,
				Title:           title,
				Description:     description,
				PublicationDate: publicationDate,
				Section:         section,
				Image:           imageValue,
				IsPaid:          isPaid,
				Content:         content,
			},
		})
	}

	// Insert rows into BigQuery
	if err := table.Inserter().Put(ctx, rows); err != nil {
		logger.LogError("Failed to insert rows into BigQuery: %v", err)
		return
	}

	logger.LogInfo("Data inserted successfully")
}
