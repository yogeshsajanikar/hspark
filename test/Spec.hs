module Main(main) where

    import Test.QuickCheck
    import Test.Framework (defaultMain)
    import Test.Framework.Providers.QuickCheck2
    import Spark.RDD

    -- | Splits into number of partitions, the number of partitions
    -- are limited to cardinality of the input. Each partition can
    -- have size at least $m$ or $m+1$, where $m = input_length / n$. 
    prop_Splits :: (Positive Int,[Int]) -> Property
    prop_Splits (Positive n,ps) =
        let l = length ps
            s = splits n ps
            m = min n l
            o = l `div` m
            lcheck xs = length xs == o || length xs == (o+1) 
        in ps == (concat s) .&&. m == length s .&&. and (map lcheck s)

    
    
    main :: IO ()
    main = defaultMain [
                   testProperty "splits" prop_Splits
                  ]
           
